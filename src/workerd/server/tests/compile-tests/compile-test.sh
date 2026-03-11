#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

# $1 -> workerd binary path
# $2 -> path to desired file to compile
# $3 -> desired output

# Help Function
function show_help() {
  echo "
The Compile Test script is designed to aid in testing compiled workerd binaries.
It works by verifying that the result of an HTTP request on a running compiled binary matches an expected output.
usage: compile-test.sh [-h] <workerd command> <file-to-compile> <expected-output-file>
  options:
    -h this help message

  Note: all flags must occur before arguments
"
}

while getopts "h" option; do
  case ${option} in
    h)
      show_help
      exit
      ;;
  esac
done

shift $(expr $OPTIND - 1)
WORKERD_BINARY=$1
CAPNP_SOURCE=$2
EXPECTED=$3

CAPNP_BINARY=$(mktemp)
PORT_FILE=$(mktemp)

# Compile the app
$WORKERD_BINARY compile $CAPNP_SOURCE > $CAPNP_BINARY

# Run the app (config socket binds to the address specified in the config)
$CAPNP_BINARY --control-fd=1 > $PORT_FILE &
KILL=$!

# Make intermediate files before trying to wait on the bindings
OUTPUT=$(mktemp)
FIXED_OUTPUT=$(mktemp)
FIXED_EXPECTED=$(mktemp)

# Wait on the port bindings to occur
while ! grep \"socket\"\:\"http\" $PORT_FILE; do
  sleep .1
done

# Identify the port chosen by the binary
PORT=`grep \"socket\"\:\"http\" $PORT_FILE | sed 's/^.*"port"://g' | sed 's/\}//g' |head -n 1`

# Request output using PROXY v2 header + HTTP via python3
python3 -c "
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', int(sys.argv[1])))
# PROXY v2 header: signature(12) + v2/LOCAL(1) + AF_UNSPEC(1) + payload_len(2) + TLV(type=0xE0, len=4, 'main')
header = bytes([
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
    0x20, 0x00, 0x00, 0x07,
    0xE0, 0x00, 0x04]) + b'main'
s.sendall(header)
s.sendall(b'GET / HTTP/1.0\r\nHost: localhost\r\n\r\n')
data = b''
while True:
    chunk = s.recv(4096)
    if not chunk:
        break
    data += chunk
s.close()
# Extract body after HTTP headers
if b'\r\n\r\n' in data:
    body = data.split(b'\r\n\r\n', 1)[1]
else:
    body = data
sys.stdout.buffer.write(body)
" $PORT > $OUTPUT

# Compare the tests to the expected output
sed -e's/[[:space:]]*$//' $OUTPUT > $FIXED_OUTPUT
sed -e's/[[:space:]]*$//' $EXPECTED > $FIXED_EXPECTED
diff $FIXED_OUTPUT $FIXED_EXPECTED

# Clean up running workerd
kill -9 $KILL

# Clean up temp files
rm -f $CAPNP_BINARY
rm -f $PORT_FILE
rm -f $OUTPUT
rm -f $FIXED_OUTPUT
rm -f $FIXED_EXPECTED
