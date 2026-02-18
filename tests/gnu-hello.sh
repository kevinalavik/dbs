#!/bin/sh
set -eu

export DEBIAN_FRONTEND=noninteractive

echo "== distbuild docker sandbox smoke test =="
echo "whoami: $(whoami)"
echo "id: $(id)"
echo "pwd: $(pwd)"

if [ -f /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
fi

CODENAME="${VERSION_CODENAME:-stable}"

DEB_MIRROR="https://ftp.acc.umu.se/mirror/debian.org/debian"
SEC_MIRROR="https://ftp.acc.umu.se/mirror/debian.org/debian-security"

GNU_HELLO_MIRROR="https://ftp.acc.umu.se/mirror/gnu.org/gnu/hello/"

echo "debian_suite: ${CODENAME}"
echo "debian_mirror: ${DEB_MIRROR}"
echo "security_mirror: ${SEC_MIRROR}"
echo "gnu_hello_mirror: ${GNU_HELLO_MIRROR}"

echo "== configure APT to use Swedish mirror =="
cat > /etc/apt/sources.list <<EOF
deb ${DEB_MIRROR} ${CODENAME} main
deb ${DEB_MIRROR} ${CODENAME}-updates main
deb ${SEC_MIRROR} ${CODENAME}-security main
EOF

echo "== apt-get update =="
apt-get update

echo "== install tools (ping/curl/build) =="
apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  iputils-ping \
  build-essential \
  pkg-config \
  tar \
  xz-utils \
  gzip \
  bzip2 \
  file

echo "== network checks =="
ping -c 1 1.1.1.1
curl -fsSLI https://deb.debian.org/ >/dev/null
curl -fsSLI "${GNU_HELLO_MIRROR}" >/dev/null

echo "== fetch latest GNU hello tarball from Swedish mirror =="
html="$(curl -fsSL "${GNU_HELLO_MIRROR}")"

tarball="$(printf "%s" "${html}" | grep -Eo 'hello-[0-9]+(\.[0-9]+)*\.tar\.xz' | sort -V | tail -n 1 || true)"
if [ -z "${tarball}" ]; then
  tarball="$(printf "%s" "${html}" | grep -Eo 'hello-[0-9]+(\.[0-9]+)*\.tar\.gz' | sort -V | tail -n 1 || true)"
fi
if [ -z "${tarball}" ]; then
  echo "error: could not find hello-*.tar.(xz|gz) in mirror listing" >&2
  exit 2
fi

echo "tarball: ${tarball}"
curl -fSLo "${tarball}" "${GNU_HELLO_MIRROR}${tarball}"
ls -lh "${tarball}"
file "${tarball}"

echo "== extract =="
case "${tarball}" in
  *.tar.xz) tar -xJf "${tarball}" ;;
  *.tar.gz) tar -xzf "${tarball}" ;;
  *)
    echo "error: unknown archive format: ${tarball}" >&2
    exit 3
    ;;
esac

srcdir="${tarball%.tar.*}"
if [ ! -d "${srcdir}" ]; then
  echo "error: expected source dir not found: ${srcdir}" >&2
  exit 4
fi

echo "== build from source =="
cd "${srcdir}"
./configure --prefix=/usr/local
make -j "$(nproc)"
make install

echo "== run =="
/usr/local/bin/hello --version
/usr/local/bin/hello "hej"

echo "== done =="
