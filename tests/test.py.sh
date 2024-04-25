set -euo pipefail

ROOT=$(dirname $(dirname $(realpath -s $0)))

cargo build -r -p py
pushd $ROOT/target/release
ln -s libpolarhouse.so polarhouse.so || true
popd

export PYTHONPATH=$ROOT/target/release
python $ROOT/tests/test.py
