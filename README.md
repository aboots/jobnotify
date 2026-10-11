# jobnotify

`jobnotify` watches your Slurm queue and sends one ntfy message when a batch of jobs has finished. Agents do not register job ids. The checker notices jobs by itself.

It uses only the Python standard library. The repository contains no tokens, topics, or account details. Each server creates its own private ntfy topic during install and prints a link.

## Install

```bash
git clone https://github.com/aboots/jobnotify.git
cd jobnotify
./install.sh
```

`install.sh` prints one URL. Give that URL to the user. That is the only server-specific secret, and it stays in `~/.config/jobnotify/config.env` with mode `600`.

## Add the link in the ntfy app

1. Install ntfy from the App Store, Play Store, or F-Droid.
2. Open the app and tap **+**.
3. Choose **Subscribe to topic**.
4. Paste the full `https://ntfy.sh/...` link, or only the topic name after the slash.
5. Confirm. A test message can be sent with `jobnotify test`.

Opening the same link in the phone browser also offers to subscribe in the app.

## What it does

A user timer runs `jobnotify check` every minute. Each check is one local `squeue` lookup.

When jobs are pending or running, they belong to the current batch. Jobs that appear later join that same batch. The batch is ready only after both of these are true:

- no new job has appeared for 10 minutes (`AUTO_QUIET_MINUTES`)
- the queue no longer has pending or running jobs

One message then names the host and the job states. It does not include logs, metrics, or file paths beyond the job name. The next time jobs appear, a new batch starts.

`jobnotify list` shows the current batch. `jobnotify link` prints the subscribe URL again.

## For an agent on a new server

1. Clone the public repository and run `./install.sh`.
2. Copy the printed `https://ntfy.sh/...` URL into the reply so the user can subscribe.
3. Do not commit `~/.config/jobnotify/config.env` and do not paste the topic into this repository.
4. Do not register individual Slurm ids. Submitting jobs is enough.
5. `jobnotify test` should succeed after the topic exists.
