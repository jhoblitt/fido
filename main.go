package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/jhoblitt/expfake/hostmap"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

var loggerOpts = &slog.HandlerOptions{
	ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Value.Kind() == slog.KindFloat64 {
			f := a.Value.Float64()
			return slog.String(a.Key, fmt.Sprintf("%.3f", f))
		}
		return a
	},
}

var logger = slog.New(slog.NewJSONHandler(os.Stdout, loggerOpts))

type conf struct {
	HostMapPath *string       `json:"hostmap"`
	Bucket      *string       `json:"bucket"`
	Port        *int          `json:"port"`
	InputDir    *string       `json:"input_dir"`
	Prefix      *string       `json:"prefix"`
	Runs        *int          `json:"runs"`
	OffsetRaw   *string       `json:"offset"`
	Offset      time.Duration `json:"-"`
}

type hostWorkerInput struct {
	host      string
	conf      *conf
	fileNames []string
	start     time.Time
	run       int
}

type float643f float64

func (f float643f) MarshalJSON() ([]byte, error) {
	// equiv of `fmt.Sprintf("%.3f", f)`
	return []byte(strconv.FormatFloat(float64(f), 'f', 3, 64)), nil
}

type runSummary struct {
	Runs   int       `json:"runs"`
	Mean   float643f `json:"mean_seconds"`
	Median float643f `json:"median_seconds"`
	Min    float643f `json:"min_seconds"`
	Max    float643f `json:"max_seconds"`
	StdDev float643f `json:"stddev_seconds"`
}

func sendFile(s3ndUrl, uri, file string, wg *sync.WaitGroup, h *hostWorkerInput) {
	go func() {
		defer wg.Done()

		//nolint:gosec // G107 -- s3ndUrl is caller validated
		resp, err := http.PostForm(s3ndUrl, url.Values{
			"file": {file},
			"uri":  {uri},
		})
		if err != nil {
			logger.Error("error sending file", "host", h.host, "file", file, "uri", uri, "err", err, "run", h.run)
			os.Exit(1)
		}

		if resp.StatusCode != 200 {
			logger.Error("error sending file", "host", h.host, "response", resp, "run", h.run)
			os.Exit(1)
		}
	}()
}

func hostWorker(h *hostWorkerInput) {
	var wg sync.WaitGroup

	s3ndUrl := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%v:%d", h.host, *h.conf.Port),
		Path:   "/upload",
	}

	for _, fName := range h.fileNames {
		fullFilePath := filepath.Join(*h.conf.InputDir, fName)

		// filter out non-json files
		if filepath.Ext(fullFilePath) != ".json" {
			continue
		}

		uri := fmt.Sprintf("s3://%v/%v/%v", *h.conf.Bucket, *h.conf.Prefix, fName)

		wg.Add(1)
		sendFile(s3ndUrl.String(), uri, fullFilePath, &wg, h)
	}

	wg.Wait()
	logger.Info(".json done", "host", h.host, "duration_seconds", time.Since(h.start).Seconds(), "run", h.run)

	for _, fName := range h.fileNames {
		fullFilePath := filepath.Join(*h.conf.InputDir, fName)

		// filter out json files
		if filepath.Ext(fullFilePath) == ".json" {
			continue
		}

		uri := fmt.Sprintf("s3://%v/%v/%v", *h.conf.Bucket, *h.conf.Prefix, fName)

		wg.Add(1)
		sendFile(s3ndUrl.String(), uri, fullFilePath, &wg, h)
	}

	wg.Wait()
	logger.Info("host done", "host", h.host, "duration_seconds", time.Since(h.start).Seconds(), "run", h.run)
}

func summarizeRunResults(runResults []float64) *runSummary {
	// sort the results to calculate median
	slices.Sort(runResults)
	median := stat.Quantile(0.5, stat.Empirical, runResults, nil)

	return &runSummary{
		Runs:   len(runResults),
		Mean:   float643f(stat.Mean(runResults, nil)),
		Median: float643f(median),
		Min:    float643f(floats.Min(runResults)),
		Max:    float643f(floats.Max(runResults)),
		StdDev: float643f(stat.StdDev(runResults, nil)),
	}
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
	}

	flag.Parse()

	slog.SetDefault(logger)

	if *conf.InputDir == "" {
		log.Fatal("dir flag is required")
	}

	offset, err := time.ParseDuration(*conf.OffsetRaw)
	if err != nil {
		log.Fatal(err)
	}
	conf.Offset = offset

	hMap := hostmap.Parse(conf.HostMapPath)

	var wg sync.WaitGroup
	runResults := make([]float64, 0, *conf.Runs)

	// start timing as late as possible to avoid including hostmap parsing time
	for i, next := 1, time.Now(); i <= *conf.Runs; i++ {
		next = next.Add(offset)

		start := time.Now()
		logger.Info("start run", "run", i)

		for hostname, fileNames := range hMap.Hosts {
			wg.Add(1)
			go func() {
				defer wg.Done()
				hostWorker(&hostWorkerInput{
					host:      hostname,
					conf:      conf,
					fileNames: fileNames,
					start:     start,
					run:       i,
				})
			}()
		}

		wg.Wait()
		duration := time.Since(start).Seconds()
		runResults = append(runResults, duration)
		logger.Info("all hosts done", "duration_seconds", duration, "run", i)

		if i < *conf.Runs {
			time.Sleep(time.Until(next))
		}
	}

	logger.Info("all runs done")

	summary := summarizeRunResults(runResults)

	logger.Info("summary of all runs", "summary", summary, "config", conf)
}
