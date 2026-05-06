# Deploying the KPS demo server

A minimal recipe for putting `kps-demo-server` on a Linux box with
systemd. Mirrors the layout used on `ef-multi`.

## Layout on the target box

```
/opt/kps-demo/
  kps-demo-server        # cross-compiled binary (Linux/amd64)
  state.json             # generated on first run; do NOT rsync from dev
                         # contains { port, tls } — owner-only 0600
/etc/systemd/system/kps-demo.service
```

`state.json` is the entire persistent state. It pins the UDP port and
the TLS cert that drives the address — replace it and the published
address changes.

## First-time install

From the dev machine:

```sh
cd ~/workspaces/kps/demo/server
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/kps-demo-server .
ssh <host> 'mkdir -p /opt/kps-demo'
rsync -av /tmp/kps-demo-server <host>:/opt/kps-demo/kps-demo-server
```

Install the service unit on the host (replace `170.64.236.147` with
the public IP you want advertised in the printed address):

```sh
ssh <host> 'cat > /etc/systemd/system/kps-demo.service <<UNIT
[Unit]
Description=KPS demo (chat + eth-rpc proxy)
Documentation=https://github.com/voltrevo/kps
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/kps-demo
ExecStart=/opt/kps-demo/kps-demo-server -ip 170.64.236.147
Restart=on-failure
RestartSec=3
StandardOutput=journal
StandardError=journal
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/opt/kps-demo
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload && systemctl enable --now kps-demo'
```

Read back the address it picked:

```sh
ssh <host> 'journalctl -u kps-demo -n 5 --no-pager | grep -oE "[0-9.]+:[0-9]+:[A-Za-z0-9_-]+"'
```

That string is what users paste into the browser. Bake it into
[`demo/web/src/main.js`](../web/src/main.js) as `DEMO_ADDR` so the
deployed web app pre-fills it.

## Service management

```sh
systemctl status kps-demo
systemctl restart kps-demo
journalctl -u kps-demo -f
```

The unit runs as root with mild hardening (`ProtectSystem=strict`,
`ProtectHome=true`, `PrivateTmp=true`, `NoNewPrivileges=true`). State
writes are confined to `/opt/kps-demo` via `ReadWritePaths`.

## Deploying new code

```sh
cd ~/workspaces/kps/demo/server
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/kps-demo-server .
rsync -av /tmp/kps-demo-server <host>:/opt/kps-demo/kps-demo-server
ssh <host> 'systemctl restart kps-demo'
```

Do **not** rsync `state.json` from dev — that would replace the
published cert/port and break every bookmark of your address. If you
truly want to rotate identity, delete `state.json` on the host before
restarting.

## Firewall

The server binds UDP on whatever port `state.json` pins. If a firewall
is added later, open that UDP port inbound. The printed address tells
you which.

## Picking the right `-ip`

`-ip` only controls what gets *printed* (and persisted into `state.json`'s
implicit address). It must be an address the public can actually reach
the box on:

- public IP for an internet-reachable box
- LAN IP if you're only testing across your network

The bind is always `0.0.0.0:<port>` (all interfaces).

## Coexisting with other services

The kps server only needs its single UDP port. Multiple kps services on
one box just need different ports — let each one auto-pick on first start
and they'll persist independently.
