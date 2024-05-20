set -euo pipefail

ROOT=$(dirname $(dirname $(realpath -s $0)))

cargo build -r -p py
pushd $ROOT/target/release
ln -s libpolarhouse.so polarhouse.so || true
popd

if [ ! -d "venv" ]; then
    python -m virtualenv venv
    source venv/bin/activate
    pip install polars==0.20.7
else    
    source venv/bin/activate
fi

export PYTHONPATH=$ROOT/target/release
python -m unittest $ROOT/tests/test.py
deactivate
