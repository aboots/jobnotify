# jobnotify

`jobnotify` watches a named set of jobs and sends one message when the whole set has finished. Use it after `sbatch`, or after starting a local command, so you can come back and read the results instead of polling the queue.

The checker runs on the machine where the jobs run. It uses only the Python standard library.

## Install

```bash
git clone git@github.com:aboots/jobnotify.git
cd jobnotify
./install.sh
```

This copies the command to `~/.local/bin/jobnotify`, creates `~/.config/jobnotify/config.env` the first time, and starts a user systemd timer that checks every minute.

Logout keeps the timer running when lingering is enabled. If this machine cannot enable lingering, the checker runs while you still have a login session. A cron line that does the same thing is:

```bash
* * * * * $HOME/.local/bin/jobnotify check
```

## Choose a channel

Edit `~/.config/jobnotify/config.env` and fill in one or more channels. `jobnotify channels` shows what is ready. `jobnotify test` sends a test message.

### Phone push

Install [ntfy](https://ntfy.sh) on your phone, or open the topic in a browser. Put a long random topic in the config:

```bash
NTFY_SERVER=https://ntfy.sh
NTFY_TOPIC=replace-with-a-long-random-string
```

Subscribe to that same topic. Anyone who knows the topic can read the alerts, so keep the config file private (`chmod 600`).

### Telegram

Create a bot with [@BotFather](https://t.me/BotFather), then send any message to that bot. Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. The chat id is the `id` field from `https://api.telegram.org/bot<token>/getUpdates`.

### Gmail

Create an app password in Google Account, Security, App passwords. Gmail rejects the normal account password for SMTP.

```bash
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_SECURITY=starttls
SMTP_USER=you@gmail.com
SMTP_PASSWORD=the-app-password
SMTP_FROM=you@gmail.com
SMTP_TO=you@gmail.com
```

Any other SMTP server uses the same keys.

### WhatsApp

[CallMeBot](https://www.callmebot.com/blog/free-api-whatsapp-messages/) gives a small API key. Put this in `WEBHOOK_URL`, with your phone number and key:

```bash
WEBHOOK_URL=https://api.callmebot.com/whatsapp.php?phone=1XXXXXXXXXX&apikey=YOURKEY&text={message}
```

A webhook without `{message}` receives a JSON `POST` of `{"title", "body"}`.

## Watch a protocol

Give every experiment a name. The alert includes that name, the host, and each job's state.

```bash
jobnotify watch 12345 12346 --name "V50 training"
```

Array jobs count as one id. Every task that is still visible to Slurm is part of the protocol. One message is sent when all of them have reached a final state.

You can also submit and watch in one step:

```bash
jobnotify sbatch --name "V50 training" -- --parsable job.slurm
```

Adding more ids to the same name keeps them in the same protocol:

```bash
jobnotify watch 12347 --name "V50 training"
```

`jobnotify list` shows what is still open. `jobnotify forget "V50 training"` drops it.

A local command works on machines without Slurm:

```bash
jobnotify run --name "local eval" -- python evaluate.py
jobnotify run --detach --name "local train" -- python train.py
```

## What the message says

A clean finish looks like:

```text
Jobs finished: V50 training
Host: your-server
3 completed

  12345  train  COMPLETED  01:12:03  exit 0:0
  ...
Come check the results.
```

If a job fails while others are still running, one early message is sent. When the rest finish, the final message is sent. Cancelled tasks are listed in the final message and do not send the early warning by themselves.

The message has job ids and states. It does not include logs, metrics, or file contents.

On a cluster whose accounting database is down, states come from the live scheduler. A job that disappears before its exit code is recorded is reported as `LEFT_QUEUE`.

## Update

```bash
git pull
./install.sh
```

Existing `config.env` is left in place.
