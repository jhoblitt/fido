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
	"sync"
	"time"

	"github.com/jhoblitt/expfake/hostmap"
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

type hostWorkerInput struct {
	host      string
	port      int
	bucket    string
	prefix    string
	inputDir  string
	fileNames []string
	start     time.Time
	run       int
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
		Host:   fmt.Sprintf("%v:%v", h.host, h.port),
		Path:   "/upload",
	}

	for _, fName := range h.fileNames {
		fullFilePath := filepath.Join(h.inputDir, fName)

		// filter out non-json files
		if filepath.Ext(fullFilePath) != ".json" {
			continue
		}

		uri := fmt.Sprintf("s3://%v/%v/%v", h.bucket, h.prefix, fName)

		wg.Add(1)
		sendFile(s3ndUrl.String(), uri, fullFilePath, &wg, h)
	}

	wg.Wait()
	logger.Info(".json done", "host", h.host, "duration_seconds", time.Since(h.start).Seconds(), "run", h.run)

	for _, fName := range h.fileNames {
		fullFilePath := filepath.Join(h.inputDir, fName)

		// filter out json files
		if filepath.Ext(fullFilePath) == ".json" {
			continue
		}

		uri := fmt.Sprintf("s3://%v/%v/%v", h.bucket, h.prefix, fName)

		wg.Add(1)
		sendFile(s3ndUrl.String(), uri, fullFilePath, &wg, h)
	}

	wg.Wait()
	logger.Info("host done", "host", h.host, "duration_seconds", time.Since(h.start).Seconds(), "run", h.run)
}

func main() {
	hostMapPath := flag.String("hostmap", "hostmap.yaml", "Path to the hostmap file")
	bucket := flag.String("bucket", "rubin-summit-users", "S3 bucket to use")
	port := flag.Int("port", 15571, "s3nd port")
	inputDir := flag.String("dir", "", "Path to the directory to read files from")
	objPrefix := flag.String("prefix", "u/fido", "prefix to add to s3 object names")
	runs := flag.Int("runs", 1, "run the benchmark this many times")
	offsetRaw := flag.String("offset", "30s", "offset between the start of runs")

	flag.Parse()

	slog.SetDefault(logger)

	if *inputDir == "" {
		log.Fatal("dir flag is required")
	}

	offset, err := time.ParseDuration(*offsetRaw)
	if err != nil {
		log.Fatal(err)
	}

	hMap := hostmap.Parse(hostMapPath)

	var wg sync.WaitGroup

	// start timing as late as possible to avoid including hostmap parsing time
	for i, next := 1, time.Now(); i <= *runs; i++ {
		next = next.Add(offset)

		start := time.Now()
		logger.Info("start run", "run", i)

		for hostname, fileNames := range hMap.Hosts {
			wg.Add(1)
			go func() {
				defer wg.Done()
				hostWorker(&hostWorkerInput{
					host:      hostname,
					port:      *port,
					bucket:    *bucket,
					prefix:    *objPrefix,
					inputDir:  *inputDir,
					fileNames: fileNames,
					start:     start,
					run:       i,
				})
			}()
		}

		wg.Wait()
		logger.Info("all hosts done", "duration_seconds", time.Since(start).Seconds(), "run", i)

		if i < *runs {
			time.Sleep(time.Until(next))
		}
	}

	logger.Info("all runs done")
}
