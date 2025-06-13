package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jhoblitt/expfake/hostmap"
	"github.com/lsst-dm/s3nd/upload"
	s3ndversion "github.com/lsst-dm/s3nd/version"
	gherrors "github.com/pkg/errors"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"

	"github.com/jhoblitt/fido/version"
)

var loggerOpts = &slog.HandlerOptions{
	ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Value.Kind() == slog.KindFloat64 {
			f := a.Value.Float64()
			return slog.Any(a.Key, float643f(f))
		}
		return a
	},
}

var logger = slog.New(slog.NewJSONHandler(os.Stdout, loggerOpts))

type conf struct {
	HostMapPath *string       `json:"hostmap"`
	Bucket      *string       `json:"bucket"`
	Port        *int          `json:"port"`
	InputDir    *string       `json:"dir"`
	Prefix      *string       `json:"prefix"`
	Runs        *int          `json:"runs"`
	OffsetRaw   *string       `json:"offset"`
	Offset      time.Duration `json:"-"`
	WarmupRuns  *int          `json:"warmup-runs"`
	SendFirst   *string       `json:"send-first"`
}

type hostWorkerInput struct {
	host      string
	conf      *conf
	fileNames []string
	start     time.Time
	run       int
}

type fileStatus struct {
	s3ndUrl       url.URL
	uri           url.URL
	file          string
	requestStatus *upload.RequestStatus
	error         error
}

type float643f float64

func (f float643f) MarshalJSON() ([]byte, error) {
	// JSON uses "null" for NaN values
	if math.IsNaN(float64(f)) {
		return []byte("null"), nil
	}
	// equiv of `fmt.Sprintf("%.3f", f)`
	return []byte(strconv.FormatFloat(float64(f), 'f', 3, 64)), nil
}

type benchmarkSummary struct {
	Runs   int       `json:"runs"`
	Mean   float643f `json:"mean_seconds"`
	Median float643f `json:"median_seconds"`
	Min    float643f `json:"min_seconds"`
	Max    float643f `json:"max_seconds"`
	StdDev float643f `json:"stddev_seconds"`
}

type versionInfo struct {
	Version string `json:"version"`
	Config  conf   `json:"config"`
}

func sendFile(s3ndUrl, uri url.URL, file string) (*upload.RequestStatus, error) {
	//nolint:gosec // G107 -- s3ndUrl is caller validated
	r, err := http.PostForm(s3ndUrl.String(), url.Values{
		"file": {file},
		"uri":  {uri.String()},
	})
	if err != nil {
		return nil, gherrors.Wrapf(err, "error sending file %q to uri %q", file, uri.String())
	}
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, gherrors.Errorf("error reading response from s3nd: %v", err)
	}
	status := upload.RequestStatus{}
	err = json.Unmarshal(body, &status)
	if err != nil {
		return nil, gherrors.Errorf("error unmarshalling response from s3nd: %v", err)
	}

	if r.StatusCode != 200 {
		return &status, gherrors.Errorf("error sending file %q to uri %q, msg %q", file, uri.String(), status.Msg)
	}

	return &status, nil
}

// Send multiple files to the s3nd service in parallel.
func sendFiles(s3ndUrl url.URL, files map[string]url.URL) (*[]*fileStatus, error) {
	var wg sync.WaitGroup
	statusCh := make(chan *fileStatus, len(files))
	for f, uri := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, err := sendFile(s3ndUrl, uri, f)
			statusCh <- &fileStatus{
				s3ndUrl:       s3ndUrl,
				uri:           uri,
				file:          f,
				requestStatus: status,
				error:         err,
			}
		}()
	}

	wg.Wait()
	close(statusCh)

	var statuses []*fileStatus
	var errs []error
	for s := range statusCh {
		statuses = append(statuses, s)
		if s.error != nil {
			// XXX
			logger.Error("error", "error", s.error)
			errs = append(errs, s.error)
		}
	}
	// XXX
	logger.Info("sendFiles finished", "files_count", len(files), "errors_count", len(errs))

	if len(errs) > 0 {
		return &statuses, errors.Join(errs...)
	}

	return &statuses, nil
}

func hostWorker(h *hostWorkerInput) error {
	var sendFilter *regexp.Regexp

	s3ndUrl := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%v:%d", h.host, *h.conf.Port),
		Path:   "/upload",
	}

	if *h.conf.SendFirst != "" {
		sendFilter = regexp.MustCompile(*h.conf.SendFirst)

		files := make(map[string]url.URL, len(h.fileNames))
		for _, fName := range h.fileNames {
			fullFilePath := filepath.Join(*h.conf.InputDir, fName)

			// filter out non-json files
			if !sendFilter.MatchString(filepath.Ext(fullFilePath)) {
				continue
			}

			files[fullFilePath] = url.URL{
				Scheme: "s3",
				Host:   *h.conf.Bucket,
				Path:   fmt.Sprintf("%v/%v", *h.conf.Prefix, fName),
			}
		}

		// XXX
		statuses, err := sendFiles(s3ndUrl, files)
		if err != nil {
			logger.Error(fmt.Sprintf("%v failed", *h.conf.SendFirst), "host", h.host, "duration_seconds", time.Since(h.start).Seconds(), "run", h.run, "error", err)
			return err
		}

		// logs number of files sent and total number of bytes
		var totalBytes int64
		for _, s := range *statuses {
			if s.error != nil {
				continue
			}
			totalBytes += s.requestStatus.Task.SizeBytes
		}

		logger.Info(fmt.Sprintf("%v done", *h.conf.SendFirst), "host", h.host, "duration_seconds", time.Since(h.start).Seconds(), "run", h.run, "total_size_bytes", totalBytes, "files_count", len(*statuses))
	}

	files := make(map[string]url.URL, len(h.fileNames))
	for _, fName := range h.fileNames {
		fullFilePath := filepath.Join(*h.conf.InputDir, fName)

		// filter out json files
		if *h.conf.SendFirst != "" && sendFilter.MatchString(filepath.Ext(fullFilePath)) {
			continue
		}

		files[fullFilePath] = url.URL{
			Scheme: "s3",
			Host:   *h.conf.Bucket,
			Path:   fmt.Sprintf("%v/%v", *h.conf.Prefix, fName),
		}
	}

	// XXX
	statuses, err := sendFiles(s3ndUrl, files)
	if err != nil {
		logger.Error(fmt.Sprintf("%v failed", *h.conf.SendFirst), "host", h.host, "duration_seconds", time.Since(h.start).Seconds(), "run", h.run, "error", err)
		return err
	}

	// logs number of files sent and total number of bytes
	var totalBytes int64
	for _, s := range *statuses {
		if s.error != nil {
			continue
		}
		totalBytes += s.requestStatus.Task.SizeBytes
	}

	logger.Info(fmt.Sprintf("%v done", *h.conf.SendFirst), "host", h.host, "duration_seconds", time.Since(h.start).Seconds(), "run", h.run, "total_size_bytes", totalBytes, "files_count", len(*statuses))

	logger.Info("host finished", "host", h.host, "duration_seconds", time.Since(h.start).Seconds(), "run", h.run)

	return nil
}

func run(hMap *hostmap.HostMap, conf *conf, i int) (time.Duration, error) {
	logger.Info("start run", "run", i)

	start := time.Now()

	var wg sync.WaitGroup
	errCh := make(chan error, len(hMap.Hosts))
	for hostname, fileNames := range hMap.Hosts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- hostWorker(&hostWorkerInput{
				host:      hostname,
				conf:      conf,
				fileNames: fileNames,
				start:     start,
				run:       i,
			})
		}()
	}
	wg.Wait()
	close(errCh)
	duration := time.Since(start)

	var errs []error
	for e := range errCh {
		if e != nil {
			logger.Error("error", "error", e)
			errs = append(errs, e)
		}
	}

	if len(errs) > 0 {
		logger.Error("run failed", "duration_seconds", duration.Seconds(), "run", i, "errors", errs)
		return duration, errors.Join(errs...)
	}

	logger.Info("run finished", "duration_seconds", duration.Seconds(), "run", i)

	return duration, nil
}

func runBenchmark(hMap *hostmap.HostMap, conf *conf, runs int) []float64 {
	runResults := make([]float64, 0, runs)

	// start timing as late as possible to exclude setup time
	for i, next := 1, time.Now(); i <= runs; i++ {
		next = next.Add(conf.Offset)

		duration, err := run(hMap, conf, i)
		if err != nil {
			logger.Error("error during benchmark", "error", err)
		}
		runResults = append(runResults, duration.Seconds())

		if i < runs {
			time.Sleep(time.Until(next))
		}
	}

	return runResults
}

func summarizeBenchmarkResults(runResults []float64) *benchmarkSummary {
	// sort the results to calculate median
	slices.Sort(runResults)
	median := stat.Quantile(0.5, stat.Empirical, runResults, nil)

	return &benchmarkSummary{
		Runs:   len(runResults),
		Mean:   float643f(stat.Mean(runResults, nil)),
		Median: float643f(median),
		Min:    float643f(floats.Min(runResults)),
		Max:    float643f(floats.Max(runResults)),
		StdDev: float643f(stat.StdDev(runResults, nil)),
	}
}

func collectVersionInfo(hMap *hostmap.HostMap, conf *conf) (map[string]s3ndversion.VersionInfo, error) {
	hostInfo := make(map[string]s3ndversion.VersionInfo, len(hMap.Hosts))
	for hostname := range hMap.Hosts {
		r, err := http.Get(fmt.Sprintf("http://%s:%d/version", hostname, *conf.Port))
		if err != nil {
			return nil, gherrors.Wrapf(err, "error checking version on host %s", hostname)
		}
		defer r.Body.Close()

		if r.StatusCode != http.StatusOK {
			return nil, gherrors.Errorf("s3nd on host %s returned status %d, expected 200", hostname, r.StatusCode)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, gherrors.Errorf("error reading version info from host %s: %v", hostname, err)
		}
		vInfo := s3ndversion.VersionInfo{}
		err = json.Unmarshal(body, &vInfo)
		if err != nil {
			return nil, gherrors.Errorf("error unmarshalling version info from host %s: %v", hostname, err)
		}

		hostInfo[hostname] = vInfo
	}

	return hostInfo, nil
}

// Compare all versions of s3nd running on the hosts and error if there are
// differences.
func checkVersionInfo(m map[string]s3ndversion.VersionInfo) error {
	if len(m) == 0 {
		return nil
	}

	// extract keys, map iteration is random
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// use the first key as the baseline for cmp
	baselineKey := keys[0]
	baseline := m[baselineKey]

	for _, k := range keys[1:] {
		cur := m[k]

		if diff := cmp.Diff(baseline, cur); diff != "" {
			return gherrors.Errorf("config mismatch between hosts %v and %v: %v", baselineKey, k, diff)
		}
	}

	return nil
}

func main() {
	conf := &conf{
		HostMapPath: flag.String("hostmap", "hostmap.yaml", "Path to the hostmap file"),
		Bucket:      flag.String("bucket", "rubin-summit-users", "S3 bucket to use"),
		Port:        flag.Int("port", 15571, "s3nd port"),
		InputDir:    flag.String("dir", "", "Path to the directory to read files from"),
		Prefix:      flag.String("prefix", "u/fido", "prefix to add to s3 object names"),
		Runs:        flag.Int("runs", 1, "run the benchmark this many times"),
		OffsetRaw:   flag.String("offset", "30s", "offset between the start of runs"),
		WarmupRuns:  flag.Int("warmup-runs", 0, "run this many warmup runs and discard the results"),
		SendFirst:   flag.String("send-first", "", "files matching this regular expression will be uploaded before all other files"),
	}

	versionFlag := flag.Bool("version", false, "print version and exit")

	flag.Parse()

	slog.SetDefault(logger)

	if *versionFlag {
		fmt.Println(version.Version)
		os.Exit(0)
	}

	logger.Info("fido version", "version", version.Version)

	if *conf.InputDir == "" {
		log.Fatal("dir flag is required")
	}

	offset, err := time.ParseDuration(*conf.OffsetRaw)
	if err != nil {
		log.Fatal(err)
	}
	conf.Offset = offset

	hMap := hostmap.Parse(conf.HostMapPath)

	// collect conf of all s3nd hosts
	hostInfo, err := collectVersionInfo(hMap, conf)
	if err != nil {
		log.Fatalf("error collecting version info: %v", err)
	}
	// require all s3nd hosts to have the same version & config
	err = checkVersionInfo(hostInfo)
	if err != nil {
		log.Fatalf("version mismatch between hosts: %v", err)
	}

	if *conf.WarmupRuns > 0 {
		logger.Info("start warmup runs", "warmup_runs", *conf.WarmupRuns)
		_ = runBenchmark(hMap, conf, *conf.WarmupRuns)
		logger.Info("all warmup runs done")
		// don't start benchmark runs immediately after the last warmup run
		time.Sleep(conf.Offset)
	}

	logger.Info("start benchmark runs", "runs", *conf.Runs)
	benchmarkResults := runBenchmark(hMap, conf, *conf.Runs)
	logger.Info("all bencharmk runs done")

	summary := summarizeBenchmarkResults(benchmarkResults)

	var s3ndConf s3ndversion.VersionInfo
	for _, v := range hostInfo {
		s3ndConf = v
		break // take the first key, all are identical
	}

	fidoConf := versionInfo{
		Version: version.Version,
		Config:  *conf,
	}

	logger.Info("summary of all runs", "summary", summary, "fido", fidoConf, "s3nd", s3ndConf)
}
