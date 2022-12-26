#!/bin/bash
set -e

# a helper script to run tests

if ! which nsqd >/dev/null; then
  echo "missing nsqd binary" && exit 1
fi

if ! which nsqlookupd >/dev/null; then
  echo "missing nsqlookupd binary" && exit 1
fi

# run nsqlookupd
LOOKUP_LOGFILE=$(mktemp -t nsqlookupd.XXXXXXX)
echo "starting nsqlookupd"
echo "  logging to $LOOKUP_LOGFILE"
nsqlookupd >$LOOKUP_LOGFILE 2>&1 &
LOOKUPD_PID=$!

# run nsqd configured to use our lookupd above
rm -f *.dat
NSQD_LOGFILE=$(mktemp -t nsqlookupd.XXXXXXX)
echo "starting nsqd --data-path=/tmp --lookupd-tcp-address=127.0.0.1:4160 --tls-cert=./test/server.pem --tls-key=./test/server.key --tls-root-ca-file=./test/ca.pem"
echo "  logging to $NSQD_LOGFILE"
nsqd --data-path=/tmp --lookupd-tcp-address=127.0.0.1:4160 --tls-cert=./test/server.pem --tls-key=./test/server.key --tls-root-ca-file=./test/ca.pem >$NSQD_LOGFILE 2>&1 &
NSQD_PID=$!

NSQD_UNIX_SOCKET_LOGFILE=$(mktemp -t nsqlookupd.XXXXXXX)
NSQD_UNIX_SOCKET_DATA_PATH=$(mktemp -d tmp.XXXXXXX)
echo "starting nsqd with unix socket --use-unix-sockets --tcp-address /tmp/nsqd.sock --https-address /tmp/nsqd-https.sock --data-path=$NSQD_UNIX_SOCKET_DATA_PATH --tls-cert=./test/server.pem --tls-key=./test/server.key --tls-root-ca-file=./test/ca.pem"
echo "  logging to $NSQD_UNIX_SOCKET_LOGFILE"
nsqd --use-unix-sockets --tcp-address /tmp/nsqd.sock --http-address /tmp/nsqd-http.sock --https-address /tmp/nsqd-https.sock --data-path=$NSQD_UNIX_SOCKET_DATA_PATH --tls-cert=./test/server.pem --tls-key=./test/server.key --tls-root-ca-file=./test/ca.pem >$NSQD_UNIX_SOCKET_LOGFILE 2>&1 &
NSQD_UNIX_SOCKET_PID=$!

sleep 0.3

cleanup() {
  echo "killing nsqd PID $NSQD_PID"
  kill -s TERM $NSQD_PID || cat $NSQD_LOGFILE
  echo "killing nsqd unix socket PID $NSQD_UNIX_SOCKET_PID"
  kill -s TERM $NSQD_UNIX_SOCKET_PID || cat $NSQD_LOGFILE
  echo "killing nsqlookupd PID $LOOKUPD_PID"
  kill -s TERM $LOOKUPD_PID || cat $LOOKUP_LOGFILE

  rm -f /tmp/nsqd.sock
  rm -f /tmp/nsqd-https.sock
}
trap cleanup INT TERM EXIT

go test -v -timeout 60s
