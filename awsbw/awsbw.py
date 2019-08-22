#!/usr/bin/env python3
import boto3
import curses
from curses import wrapper
from curses import panel
import sys
import argparse
import time
from datetime import datetime
from multiprocessing import Process, Manager
from collections import Counter


class AWSBW():
    def __init__(
            self,
            stdscr,
            jobQueues,
            max_age_days=7,
            aws_profile='default',
            job_polling_sec=60):
        self.__currentJobs__ = []
        try:
            self.__max_age_days__ = int(max_age_days)
        except:
            self.__max_age_days__ = 7
        try:
            self.__job_polling_sec__ = int(job_polling_sec)
        except:
            self.__job_polling_sec__ = 30
        self.__aws_profile__ = aws_profile
        # screen stuff
        try:
            curses.curs_set(0)
        except:
            pass
        self.__stdscr__ = stdscr
        self.__stdscr__.nodelay(True)
        (curH, curW) = stdscr.getmaxyx()
        self.__stdscr__.clear()

        # add a window for the job listing
        self.__jobsWin__ = curses.newwin(
            curH - 2,
            curW - 2,
            1, 1
        )
        self.__termHeight__ = None
        self.__termWidth__ = None

        # Job stuff
        self.__jobStatuses__ = [
            'RUNNING',
            'RUNNABLE',
            'SUCCEEDED',
            'FAILED',
            'STARTING',
        ]
        self.__jobQueues__ = jobQueues
        self.__curJobQueue__ = jobQueues[0]
        self.__curJobId__ = None
        self.__lastJobCheck__ = None
        # Now display!
        self.screenRefresh()

    def screenRefresh(self, forceRedraw=False):
        (curH, curW) = self.__stdscr__.getmaxyx()
        if self.__termHeight__ != curH or self.__termWidth__ != curW:
            curses.resizeterm(curH, curW)
            self.__termHeight__ = curH
            self.__termWidth__ = curW
            self.__jobsWin__.resize(
                curH - 2,
                curW - 2,
            )
            self.__stdscr__.border()
            self.__stdscr__.refresh()
            self.showJobs()
        elif forceRedraw:
            self.__stdscr__.border()
            self.__stdscr__.refresh()
            self.showJobs()

        # Header: Use it to show the queues including which is current.
        x = 1
        for q in self.__jobQueues__:
            if x + len(q) > curW:
                break
            if q == self.__curJobQueue__:
                self.__stdscr__.addstr(
                    0, x,
                    q,
                    curses.A_UNDERLINE
                )
                x += len(q) + 1
            else:
                self.__stdscr__.addstr(
                    0, x,
                    q,
                )
                x += len(q) + 1

        if x + 20 < curW and self.__lastJobCheck__ is not None:
            # If we have space, add the timestamp of the last check
            self.__stdscr__.addstr(
                0, curW - 20,
                datetime.fromtimestamp(
                    self.__lastJobCheck__).strftime('%Y-%m-%d %H:%M:%S')
            )

        # Footer
        if curW > 71:
            self.__stdscr__.addstr(
                curH - 1,
                max(
                    1,
                    int(curW / 2) - 34
                ),
                " < > queues. D details. L logs. T terminate. Q quit. "
            )
        self.__stdscr__.refresh()

    def showJobs(self, moveKey=None):
        win = self.__jobsWin__

        # Limit to the current queue and recency:
        cutoff_ts = (time.time() - self.__max_age_days__ * 24 * 3600) * 1000

        jobs = [
            j for j in
            self.__currentJobs__
            if j['queue'] == self.__curJobQueue__ and j['createdAt'] >= cutoff_ts
        ]

        (winH, winW) = win.getmaxyx()

        if len(jobs) == 0:
            if self.__lastJobCheck__ is None:
                win.erase()
                win.addnstr(
                    1,
                    0,
                    "Loading Jobs...",
                    winW
                )
                win.refresh()
                return
            else:
                win.erase()
                win.addnstr(
                    1,
                    0,
                    "No Jobs",
                    winW
                )
                win.refresh()
                return

        statuses = [s for s in self.__jobStatuses__ if s in {j['status'] for j in jobs}]

        col_width = max([
            max(len(s) + 1 for s in statuses),
            max(len(j['jobName']) + 1 for j in jobs),
        ])

        maxJobs = winH - 2
        maxCols = int((winW - 2) / col_width)

        if self.__curJobId__ not in [j['jobId'] for j in jobs if j['status'] in statuses[0:maxCols]]:
            selected_status_i = 0
            selected_job_i = 0
        else:
            # Job ID is in our list, search for it
            curJob = [j for j in jobs if j['jobId'] == self.__curJobId__][0]
            selected_status_i = statuses.index(curJob['status'])
            selected_job_i = [j['jobId'] for j in jobs if j['status'] == curJob['status']].index(curJob['jobId'])
            # Do a bit of screen geometry sanity here
            if selected_status_i > maxCols:
                selected_status_i = 0
                selected_job_i = 0
            if selected_job_i > maxJobs:
                selected_status_i = 0
                selected_job_i = 0

        if moveKey is not None:
            if moveKey == curses.KEY_UP:
                selected_job_i = max([
                    0,
                    selected_job_i - 1
                ])
            elif moveKey == curses.KEY_DOWN:
                selected_job_i = min([
                    selected_job_i + 1,
                    maxJobs,
                    len([j for j in jobs if j['status'] == statuses[selected_status_i]]) - 1
                ])
            elif moveKey == curses.KEY_RIGHT:
                selected_status_i = min(
                    len(statuses) - 1,
                    maxCols,
                    selected_status_i + 1
                )
                selected_job_i = min([
                    selected_job_i,
                    len([j for j in jobs if j['status'] == statuses[selected_status_i]]) - 1
                ])
            elif moveKey == curses.KEY_LEFT:
                selected_status_i = max(
                    0,
                    selected_status_i - 1
                )
                selected_job_i = min([
                    selected_job_i,
                    len([j for j in jobs if j['status'] == statuses[selected_status_i]]) - 1
                ])

        win.addnstr(
            0,
            0,
            "".join([s.ljust(col_width) for s in statuses[:maxCols]]).ljust(winW),
            winW,
            curses.A_UNDERLINE
        )
        for status_i, status in enumerate(statuses):
            if status_i >= maxCols:
                break
            status_jobs = [j for j in jobs if j['status'] == status]
            for job_i, job in enumerate(status_jobs):
                if job_i > maxJobs:
                    break
                if (job_i == selected_job_i) and (status_i == selected_status_i):
                    self.__curJobId__ = job['jobId']
                    win.addnstr(
                        job_i + 1,
                        col_width * status_i,
                        job['jobName'].ljust(col_width),
                        winW,
                        curses.A_REVERSE
                    )
                else:
                    win.addnstr(
                        job_i + 1,
                        col_width * status_i,
                        job['jobName'].ljust(col_width),
                        winW
                    )
            # Clearing out the remainder of the column
            for y in range(job_i + 2, winH):
                win.addnstr(
                    y,
                    col_width * status_i,
                    "".ljust(col_width),
                    winW
                )

        # Clearing the right column
        right_pad = winW - col_width * len(statuses[:maxCols]) - 1
        if right_pad > 0:
            for y in range(1, winH):
                win.addnstr(
                    y,
                    col_width * len(statuses[:maxCols]),
                    "".ljust(right_pad),
                    winW
                )

        win.refresh()

    def queueJobs(self, queue, status='RUNNING'):
        session = boto3.session.Session(profile_name=self.__aws_profile__)
        batch_client = session.client('batch')

        jobs_QS = batch_client.list_jobs(
            jobQueue=queue,
            jobStatus=status,
        )
        JSL = jobs_QS.get('jobSummaryList', [])
        nextToken = jobs_QS.get('nextToken', None)
        while nextToken is not None:
            jobs_QS = batch_client.list_jobs(
                jobQueue=queue,
                jobStatus=status,
                nextToken=nextToken,
            )
            JSL += jobs_QS.get('jobSummaryList', [])
            nextToken = jobs_QS.get('nextToken', None)

        JSL.sort(key=lambda v: -v['createdAt'])
        for j in JSL:
            j.update({'queue': queue})
        return JSL

    def jobDetails(self, jobId):
        session = boto3.session.Session(profile_name=self.__aws_profile__)
        batch_client = session.client('batch')
        try:
            job_info = batch_client.describe_jobs(
                jobs=[
                    jobId,
                ]
            )['jobs'][0]
        except:
            job_info = None
        return job_info

    def terminateJob(self, jobId):
        batch_client = boto3.client('batch')
        batch_client.terminate_job(
            jobId=jobId,
            reason='Terminated by user'
        )

    def terminateJobDialog(self):
        try:
            job = [j for j in self.__currentJobs__ if j['jobId'] == self.__curJobId__][0]
        except:
            return
        p = panel.new_panel(self.__stdscr__)
        p.top()
        p.show()
        p_win = p.window()
        p_win.clear()
        p_win.border()
        p_win.nodelay(False)
        winH, winW = p_win.getmaxyx()
        question_str = "To terminate job {} type Y".format(job['jobName'])
        p_win.addnstr(
            int(winH / 2) - 1,
            max(
                1,
                int(winW / 2) - int(len(question_str) / 2),
            ),
            question_str,
            winW - 2,
        )
        p_win.refresh()

        c = p_win.getch()
        if c == 121 or c == 89:
            self.terminateJob(job['jobId'])
            p_win.addnstr(
                int(winH / 2) + 2,
                max(
                    1,
                    int(winW / 2) - 5,
                ),
                "Terminated",
                winW - 2,
                curses.A_REVERSE
            )
            p_win.refresh()
            time.sleep(1)

        p_win.nodelay(True)
        p.hide()
        self.screenRefresh(forceRedraw=True)

    def refreshJobs(self):
        if (self.__lastJobCheck__ is not None) and (time.time() - self.__lastJobCheck__ <= self.__job_polling_sec__):
            return False
        elif {j['jobId'] for j in self.__currentJobs__} != {j['jobId'] for j in self.__jobList__}:
            self.__currentJobs__ = [j for j in self.__jobList__]
            if len(self.__jobList__) != 0:
                self.__lastJobCheck__ = self.__jobProcessStatus__.get('last_check')
            self.showJobs()
            return True
        else:
            return False

    def queueRight(self):
        prior_queue = self.__curJobQueue__
        self.__curJobQueue__ = self.__jobQueues__[
            min(
                self.__jobQueues__.index(self.__curJobQueue__) + 1,
                len(self.__jobQueues__) - 1
            )]
        if prior_queue != self.__curJobQueue__:
            self.showJobs()

    def queueLeft(self):
        prior_queue = self.__curJobQueue__
        self.__curJobQueue__ = self.__jobQueues__[
            max(
                self.__jobQueues__.index(self.__curJobQueue__) - 1,
                0
            )]
        if prior_queue != self.__curJobQueue__:
            self.showJobs()

    def displayList(self, L, win, Hoffset, Hmax, Woffset, Wmax):
        L_i = 0
        for line in L:
            if Hmax <= (L_i + Hoffset):
                break
            line_chunks = [
                line[i:i + int(Wmax)]
                for i in range(
                    0,
                    len(line),
                    Wmax
                )
            ]
            for line_chunk in line_chunks:
                if Hmax <= (L_i + Hoffset):
                    break
                win.addstr(
                    L_i + Hoffset,
                    Woffset,
                    line_chunk.ljust(Wmax)
                )
                L_i += 1
        win.refresh()

    def detail_panel(self):
        try:
            job = [j for j in self.__currentJobs__ if j['jobId'] == self.__curJobId__][0]
        except:
            return

        dp = panel.new_panel(self.__stdscr__)
        dp.top()
        dp.show()
        dp_win = dp.window()
        dp_win.clear()
        dp_win.border()
        dp_win.refresh()
        winH, winW = dp_win.getmaxyx()
        if winH < 5:
            dp.hide()
            self.__stdscr__.border()
            return
        dp_win.addstr(
            winH - 1,
            int(winW / 2) - 3,
            "ESC to close"
        )

        # Title!
        dp_win.addnstr(
            1,
            1,
            "{} (id: {}) on {}. {}: {}".format(
                job['jobName'],
                job['jobId'],
                job['queue'],
                job.get('status', ""),
                job.get('statusReason', "")
            ),
            winW - 2,
        )
        # Timing
        timingStr = "Created: {}.".format(
            datetime.fromtimestamp(job['createdAt'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        )
        if 'startedAt' in job:
            timingStr += "\t Started: {}.".format(
                datetime.fromtimestamp(
                    job['startedAt'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            )
        if 'stoppedAt' in job:
            timingStr += "\t Stopped: {}.".format(
                datetime.fromtimestamp(
                    job['stoppedAt'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            )
        dp_win.addnstr(
            2,
            1,
            timingStr,
            winW - 2,
        )
        jobDetails = self.jobDetails(job['jobId'])
        if jobDetails is not None:
            dp_win.addnstr(
                3,
                1,
                "Job Description: {}".format(
                    jobDetails.get('jobDefinition', "").split('/')[-1]
                ),
                winW - 2
            )
            dp_win.addnstr(
                4,
                1,
                "Image: {}".format(
                    jobDetails.get('container', {}).get('image', "None"),
                ),
                winW - 2
            )
            dp_win.addnstr(
                5,
                1,
                "\tvcpu: {}\t\tmem: {:,} MB.".format(
                    jobDetails.get('container', {}).get('vcpus'),
                    jobDetails.get('container', {}).get('memory')
                ),
                winW - 2
            )
            cmd_start = 0
            commands = jobDetails.get('container', {}).get('command', [])
        else:
            cmd_start = 0
            commands = []
        self.displayList(
            commands[cmd_start:],
            win=dp_win,
            Hoffset=7,
            Hmax=winH - 7,
            Woffset=1,
            Wmax=winW - 2
        )

        # Detail window loop!
        while True:
            c = self.__stdscr__.getch()
            if c == 27:
                dp_win.clear()
                dp.hide()
                self.screenRefresh(forceRedraw=True)
                break
            elif c == curses.KEY_DOWN:
                if cmd_start < len(commands):
                    cmd_start += 1
                    self.displayList(
                        commands[cmd_start:],
                        win=dp_win,
                        Hoffset=7,
                        Hmax=winH - 7,
                        Woffset=1,
                        Wmax=winW - 2
                    )
            elif c == curses.KEY_UP:
                if cmd_start > 0:
                    cmd_start -= 1
                    self.displayList(
                        commands[cmd_start:],
                        win=dp_win,
                        Hoffset=7,
                        Hmax=winH - 7,
                        Woffset=1,
                        Wmax=winW - 2
                    )

    def getLog(self, jobStreamName, startFromHead=False):
        session = boto3.session.Session(profile_name=self.__aws_profile__)
        logs_client = session.client('logs')
        try:
            jobLog = logs_client.get_log_events(
                logGroupName='/aws/batch/job',
                logStreamName=jobStreamName,
                startFromHead=startFromHead,
            )
            if startFromHead:
                events = sorted(
                    jobLog['events'],
                    key=lambda e: e['timestamp']
                )
            else:
                events = sorted(
                    jobLog['events'],
                    key=lambda e: -e['timestamp']
                )
        except:
            events = []
        return events

    def log_panel(self):
        try:
            job = [j for j in self.__currentJobs__ if j['jobId'] == self.__curJobId__][0]
        except:
            return
        lp = panel.new_panel(self.__stdscr__)
        lp.top()
        lp.show()
        lp_win = lp.window()
        winH, winW = lp_win.getmaxyx()
        if winH < 5:
            lp.hide()
            self.__stdscr__.border()
            return
        lp_win.clear()
        lp_win.border()
        lp_win.addstr(
            winH - 1,
            int(winW / 2) - 3,
            "ESC to close"
        )
        # Title!
        lp_win.addnstr(
            1,
            1,
            "Logs for {} (id: {}) on {}".format(
                job['jobName'],
                job['jobId'],
                job['queue']
            ),
            winW - 2,
        )
        lp_win.addnstr(
            3,
            1,
            "Loading logs......".ljust(winW - 2),
            winW - 2,
        )
        lp_win.refresh()

        jobDetails = self.jobDetails(job['jobId'])
        if jobDetails is None:
            return

        try:
            jobStreamName = jobDetails['container']['logStreamName']
        except:
            return

        # Get the log
        startFromHead = True
        events = self.getLog(jobStreamName, startFromHead)
        event_first = 0

        self.displayList(
            [
                e['message'] for e
                in events[event_first:]
            ],
            win=lp_win,
            Hoffset=3,
            Hmax=winH - 2,
            Woffset=1,
            Wmax=winW - 2,
        )

        # Log window loop!
        while True:
            c = self.__stdscr__.getch()
            if c == 27:  # esc
                lp_win.clear()
                lp.hide()
                self.screenRefresh(forceRedraw=True)
                break
            elif c == 79 or c == 111:  # O or o
                lp_win.addnstr(
                    3,
                    1,
                    "Loading reversed logs ......".ljust(winW - 2),
                    winW - 2,
                )
                lp_win.refresh()
                startFromHead = not startFromHead
                events = self.getLog(jobStreamName, startFromHead)
                event_first = 0
                self.displayList(
                    [
                        e['message'] for e
                        in events[event_first:]
                    ],
                    win=lp_win,
                    Hoffset=3,
                    Hmax=winH - 2,
                    Woffset=1,
                    Wmax=winW - 2,
                )
            elif c == curses.KEY_NPAGE or c == 32:  # or space
                if (event_first + winH - 2) < len(events):
                    event_first += (winH - 2)
                    self.displayList(
                        [
                            e['message'] for e
                            in events[event_first:]
                        ],
                        win=lp_win,
                        Hoffset=3,
                        Hmax=winH - 2,
                        Woffset=1,
                        Wmax=winW - 2,
                    )
            elif c == curses.KEY_DOWN:
                if event_first < len(events):
                    event_first += 1
                    self.displayList(
                        [
                            e['message'] for e
                            in events[event_first:]
                        ],
                        win=lp_win,
                        Hoffset=3,
                        Hmax=winH - 2,
                        Woffset=1,
                        Wmax=winW - 2,
                    )
            elif c == curses.KEY_UP:
                if event_first > 0:
                    event_first -= 1
                    self.displayList(
                        [
                            e['message'] for e
                            in events[event_first:]
                        ],
                        win=lp_win,
                        Hoffset=3,
                        Hmax=winH - 2,
                        Woffset=1,
                        Wmax=winW - 2,
                    )

    def updateJobsLoop(self):
        last_check = None
        while True:
            if (last_check is None) or (time.time() - last_check >= self.__job_polling_sec__):
                # Update our time
                last_check = time.time()
                self.__jobProcessStatus__['last_check'] = last_check
                updatedJobs = []
                for queue in self.__jobQueues__:
                    queue_jobs = []
                    for status in self.__jobStatuses__:
                        queue_jobs += self.queueJobs(queue, status)
                    updatedJobs += sorted(queue_jobs, key=lambda j: -j['createdAt'])
                # All done updating all queues
                # Clear out the existing jobs
                del self.__jobList__[:]
                # Put in our updated job list
                self.__jobList__.extend(updatedJobs)
                # Sleep the thread until the next check is due
                sleep_time = self.__job_polling_sec__ - (time.time() - last_check)
                if sleep_time > 0:
                    time.sleep(sleep_time-1)

    def handleInput(self, c):
        if c == curses.KEY_UP or c == curses.KEY_DOWN:
            self.showJobs(c)
        if c == curses.KEY_LEFT or c == curses.KEY_RIGHT:
            self.showJobs(c)

        if c == 62 or c == 46:
            self.queueRight()
        if c == 60 or c == 44:
            self.queueLeft()

        if c == 68 or c == 100:
            self.detail_panel()

        if c == 108 or c == 76:
            self.log_panel()

        if c == 84 or c == 116:
            self.terminateJobDialog()

    def actionLoop(self):
        # Start job update thread

        with Manager() as self.__jobManager__:
            self.__jobList__ = self.__jobManager__.list()
            self.__jobProcessStatus__ = self.__jobManager__.dict()
            self.__jobProcessStatus__['last_check'] = None
            self.__jobProcess__ = Process(
                target=self.updateJobsLoop,
            )
            self.__jobProcess__.start()
            while True:
                c = self.__stdscr__.getch()
                if c == 113 or c == 81:
                    break
                elif c != -1:
                    self.handleInput(c)
                else:
                    time.sleep(0.1)
                self.refreshJobs()
                self.screenRefresh()
                if not self.__jobProcess__.is_alive():
                    raise Exception("Job Thread Died")
            self.__jobProcess__.terminate()


def start(stdscr, args):
    awsbw = AWSBW(
        stdscr,
        args.queue,
        args.max_age_days,
        args.profile,
        args.job_polling_sec
    )
    # UI action loop
    awsbw.actionLoop()


def main():
    parser = argparse.ArgumentParser(
        description="""AWS Batch Watcher
        A small utility for viewing jobs on AWS batch\n
        Please either list the available queues or provide queue(s) you wish to watch
        """
    )
    parser.add_argument(
        '-Q', '--queue',
        help='AWS batch queue(s) to monitor',
        nargs='+'
    )
    parser.add_argument(
        '-P', '--profile',
        help='AWS profile to use. (Default is default)',
        default='default'
    )
    parser.add_argument(
        '-L', '--list-queues',
        action='store_true',
        help="List available batch queues and exit"
    )
    parser.add_argument(
        '-D', '--max-age-days',
        default='7',
        type=int,
        help="Maximum job age (in days) to show. Integer only."
    )
    parser.add_argument(
        '-C', '--job-polling-sec',
        type=int,
        default='60',
        help="Seconds between polling for jobs (default 60 sec). Int only"
    )
    args = parser.parse_args()
    # Verify the profile exists

    if args.profile not in boto3.session.Session().available_profiles:
        print("AWS profile {} does not exist.".format(
            args.profile)
        )
        print("Available profiles: {}".format(
            ", ".join(boto3.session.Session().available_profiles)
        ))
        print("Exiting.")
        sys.exit(1)

    if args.list_queues:
        print("Available batch queues:")
        try:
            session = boto3.session.Session(profile_name=args.profile)
            batch_client = session.client('batch')
            queues = [
                q['jobQueueName'] for q in
                batch_client.describe_job_queues().get('jobQueues', [])
            ]
            for q in queues:
                print("\t{}".format(q))
        except Exception as e:
            print("Error loading queues from batch: {}".format(e))
        sys.exit(0)
    elif args.queue is not None:
        wrapper(start, args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
