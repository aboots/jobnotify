#!/usr/bin/env bash
# Install jobnotify for the current user and start the per-minute checker.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
HOME_DIR=${HOME:?HOME is required}

PY=""
if command -v python3 >/dev/null 2>&1; then
  PY=$(command -v python3)
elif [[ -x "${HOME_DIR}/miniconda3/bin/python3" ]]; then
  PY="${HOME_DIR}/miniconda3/bin/python3"
elif [[ -x "${HOME_DIR}/anaconda3/bin/python3" ]]; then
  PY="${HOME_DIR}/anaconda3/bin/python3"
else
  echo "python3 was not found. Install Python 3.9 or newer, then rerun ./install.sh" >&2
  exit 1
fi

if ! "$PY" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 9) else 1)'; then
  echo "python3 must be 3.9 or newer (found $("$PY" --version 2>&1))" >&2
  exit 1
fi

mkdir -p \
  "${HOME_DIR}/.local/bin" \
  "${HOME_DIR}/.local/lib/jobnotify" \
  "${HOME_DIR}/.config/jobnotify" \
  "${HOME_DIR}/.local/share/jobnotify" \
  "${HOME_DIR}/.config/systemd/user"

cp "${ROOT}/jobnotify.py" "${HOME_DIR}/.local/lib/jobnotify/jobnotify.py"
PY_Q=$(printf '%q' "$PY")
SCRIPT_Q=$(printf '%q' "${HOME_DIR}/.local/lib/jobnotify/jobnotify.py")
cat > "${HOME_DIR}/.local/bin/jobnotify" << EOF
#!/bin/sh
exec ${PY_Q} ${SCRIPT_Q} "\$@"
EOF
chmod 755 "${HOME_DIR}/.local/bin/jobnotify" "${HOME_DIR}/.local/lib/jobnotify/jobnotify.py"

if [[ ! -f "${HOME_DIR}/.config/jobnotify/config.env" ]]; then
  cp "${ROOT}/examples/config.env.example" "${HOME_DIR}/.config/jobnotify/config.env"
  chmod 600 "${HOME_DIR}/.config/jobnotify/config.env"
  echo "Created ${HOME_DIR}/.config/jobnotify/config.env"
else
  echo "Kept existing ${HOME_DIR}/.config/jobnotify/config.env"
fi

cp "${ROOT}/systemd/jobnotify.service" "${HOME_DIR}/.config/systemd/user/jobnotify.service"
cp "${ROOT}/systemd/jobnotify.timer" "${HOME_DIR}/.config/systemd/user/jobnotify.timer"

if command -v systemctl >/dev/null 2>&1 && [[ -n "${XDG_RUNTIME_DIR:-}" ]]; then
  systemctl --user daemon-reload
  systemctl --user enable --now jobnotify.timer
  echo "Started user timer jobnotify.timer"
else
  echo "systemd --user is unavailable in this shell. Run the timer later, or cron: * * * * * ${HOME_DIR}/.local/bin/jobnotify check" >&2
fi

if command -v loginctl >/dev/null 2>&1; then
  if loginctl enable-linger "${USER}" >/dev/null 2>&1; then
    echo "Enabled lingering so the checker keeps running after logout"
  else
    echo "Could not enable lingering. The checker runs while you still have a login session on this machine."
  fi
fi

BASHRC="${HOME_DIR}/.bashrc"
MARK="# jobnotify-path"
LINE='export PATH="$HOME/.local/bin:$PATH"'
if [[ -f "${BASHRC}" ]] && ! grep -q 'jobnotify-path\|\.local/bin' "${BASHRC}"; then
  tmp=$(mktemp)
  {
    printf '%s\n%s\n\n' "${MARK}" "${LINE}"
    cat "${BASHRC}"
  } > "${tmp}"
  mv "${tmp}" "${BASHRC}"
  echo "Added ~/.local/bin to PATH in .bashrc"
fi

echo "Installed ${HOME_DIR}/.local/bin/jobnotify"
echo "Subscribe to this topic in the ntfy app:"
"${HOME_DIR}/.local/bin/jobnotify" ensure-topic
