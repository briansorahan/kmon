package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	coreapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func main() {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	var (
		jobNames  StringSlice
		namespace string
	)
	flag.Var(&jobNames, "j", "Cron job name (required, can be provided multiple times).")
	flag.StringVar(&namespace, "n", "", "Kubernetes namespace (required).")
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	grp, ctx := errgroup.WithContext(context.Background())

	for _, jobName := range jobNames {
		job := jobName
		grp.Go(func() error {
			return watch(ctx, job, namespace, client)
		})
	}
	if err := grp.Wait(); err != nil {
		panic(err)
	}
}

func watch(ctx context.Context, cronJobName, namespace string, client *kubernetes.Clientset) error {
	log.Printf("monitoring %s", cronJobName)

	var activeJob string

GetJobsLoop:
	for {
		cronJob, err := client.BatchV1().CronJobs(namespace).Get(ctx, cronJobName, metav1.GetOptions{})
		if err != nil {
			panic(err)
		}
		numActive := len(cronJob.Status.Active)

		if numActive == 0 {
			time.Sleep(5 * time.Second)
			continue GetJobsLoop
		}
		if numActive != 1 {
			panic(fmt.Errorf("expected only 1 active cron job at a time, got %d", numActive))
		}
		if newActiveJob := cronJob.Status.Active[0].Name; newActiveJob != activeJob {
			// Never seen this job before.
			// Start a goroutine that will monitor this job.
			// TODO: wait for goroutines to finish if the program gets killed.
			activeJob = newActiveJob
			if err := monitor(ctx, client, namespace, activeJob); err != nil {
				panic(err)
			}
		}
		time.Sleep(5 * time.Second)
	}
}

func monitor(ctx context.Context, client *kubernetes.Clientset, namespace, jobName string) error {
	job, err := client.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting job: %w", err)
	}
	var labelSelector, sep string

	for k, v := range job.Labels {
		labelSelector += sep + k + "=" + v
		sep = ","
	}
	pods := client.CoreV1().Pods(namespace)

ListPods:
	for {
		select {
		case <-ctx.Done():
		default:
			podList, err := pods.List(ctx, metav1.ListOptions{
				LabelSelector: labelSelector,
			})
			if err != nil {
				return fmt.Errorf("listing pods: %w", err)
			}
			// If there are no running pods with the label we want then sleep and continue.
			runningPod, hasRunningPod := getRunning(podList)
			if !hasRunningPod {
				time.Sleep(50 * time.Millisecond)
				continue ListPods
			}
			// Start tailing the logs.
			stream, err := pods.GetLogs(runningPod.Name, &coreapi.PodLogOptions{Follow: true}).Stream(ctx)
			if err != nil {
				return fmt.Errorf("getting log stream: %w", err)
			}
			filename := runningPod.Name + ".logs"
			logFile, err := os.Create(filename)
			if err != nil {
				return fmt.Errorf("creating logg file: %w", err)
			}
			defer logFile.Close()

			if _, err := io.Copy(logFile, stream); err != nil {
				return fmt.Errorf("streaming logs: %w", err)
			}
			// Sometimes the pod still has running status after the log stream
			// is closed, so we wait until we see something other than running.
			var final *coreapi.Pod

		PodFinishing:
			for {
				final, err = pods.Get(ctx, runningPod.Name, metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("getting pod: %w", err)
				}
				if final.Status.Phase == coreapi.PodRunning {
					continue PodFinishing
				}
				time.Sleep(50 * time.Millisecond)
				break
			}
			if final.Status.Phase == coreapi.PodFailed {
				log.Printf("pod %s failed", runningPod.Name)
			}
			statusFile, err := os.Create(runningPod.Name + ".json")
			if err != nil {
				return fmt.Errorf("creating pod status file: %w", err)
			}
			defer statusFile.Close()

			if err := json.NewEncoder(statusFile).Encode(final); err != nil {
				return fmt.Errorf("writing pod status file: %w", err)
			}
			return nil
		}
	}
}

// getRunning will return the first running pod in the list and true, otherwise an empty Pod struct and false.
func getRunning(podList *coreapi.PodList) (coreapi.Pod, bool) {
	for _, pod := range podList.Items {
		if phase := pod.Status.Phase; phase == coreapi.PodRunning {
			return pod, true
		}
	}
	return coreapi.Pod{}, false
}

type StringSlice []string

func (ss *StringSlice) String() string {
	if ss == nil {
		return ""
	}
	return strings.Join(*ss, "|")
}

func (ss *StringSlice) Set(s string) error {
	*ss = append(*ss, s)
	return nil
}
