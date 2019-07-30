# awsbw
A small CLI utility to view jobs in an AWS batch queue

### Install

```bash
$ pip install awsbw
```

### Run

```bash
$ awsbw -Q [queue_name]
```
```bash
┌TEST-Batch-JobQueue─────────────────────────────────────────────2019-07-30 13:55:21┐
│RUNNABLE        SUCCEEDED       FAILED                                             │
│runnable-job-1  success-job1    example-mpi-job                                    │
│                success-job2    example-mpi-job                                    │
│                                                                                   │
│                                                                                   │
│                                                                                   │
│                                                                                   │
│                                                                                   │
│                                                                                   │
│                                                                                   │
│                                                                                   │
│                                                                                   │
│                                                                                   │
└─────── < > queues. D details. L logs. T terminate. Q quit. ───────────────────────┘
```
