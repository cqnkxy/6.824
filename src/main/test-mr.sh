#!/bin/bash
here=$(dirname "$0")
[[ "$here" = /* ]] || here="$PWD/$here"
export GOPATH="$here/../../"
pass=true
echo ""
echo "==> Part I"
go test -run Sequential mapreduce/...
[ $? -ne 0 ] && pass=false

echo ""
echo "==> Part II"
(cd "$here" && ./test-wc.sh > /dev/null)
[ $? -ne 0 ] && pass=false

echo ""
echo "==> Part III"
go test -run TestBasic mapreduce/...
[ $? -ne 0 ] && pass=false

echo ""
echo "==> Part IV"
go test -run Failure mapreduce/...
[ $? -ne 0 ] && pass=false

echo ""
echo "==> Part V (challenge)"
(cd "$here" && ./test-ii.sh > /dev/null)
[ $? -ne 0 ] && pass=false

rm "$here"/mrtmp.* "$here"/diff.out
[ $pass = true ] || exit 1