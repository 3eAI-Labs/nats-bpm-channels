#!/usr/bin/env bash
#
# Generates a Maven Central signing key, uploads the public half to a keyserver, and exports the
# private half in a form ready to paste into a GitHub secret.
#
# The passphrase is never exposed anywhere: it is not echoed to the screen, never passed on a
# command line (visible in ps output), and never lands in the shell history. It is handed to gpg
# only through a file descriptor.
#
#   bash scripts/gpg-setup.sh
#
set -euo pipefail

umask 077

# Writes OUTSIDE the repository by default. This repository is public; leaving the private key in
# the working directory risks publishing it permanently with a single `git add -A`.
# A different directory can be given as the first argument.
OUT_DIR="${1:-$HOME}"
KEY_FILE="$OUT_DIR/private-key.asc"
KEYSERVER="keyserver.ubuntu.com"

command -v gpg >/dev/null || { echo "ERROR: gpg is not installed (apt install gnupg)" >&2; exit 1; }

echo "== Maven Central GPG key =="
echo
echo "The name and email entered here are embedded in the key identity and become PUBLIC on the keyserver."
echo

read -r -p "Full name         : " REAL_NAME
read -r -p "Email             : " EMAIL

[[ -n "$REAL_NAME" && -n "$EMAIL" ]] || { echo "ERROR: name and email must not be empty" >&2; exit 1; }

# -s: not echoed to the terminal.
read -r -s -p "Passphrase        : " PASSPHRASE; echo
read -r -s -p "Passphrase (again): " PASSPHRASE2; echo
[[ "$PASSPHRASE" == "$PASSPHRASE2" ]] || { echo "ERROR: passphrases did not match" >&2; exit 1; }
[[ -n "$PASSPHRASE" ]] || { echo "ERROR: passphrase must not be empty — CI signing depends on it" >&2; exit 1; }

# The parameter file is created under umask 077 (owner-readable only) and removed on exit.
PARAMS="$(mktemp)"
cleanup() { rm -f "$PARAMS"; }
trap cleanup EXIT

cat > "$PARAMS" <<EOF
%echo Generating key (RSA 4096, no expiry)...
Key-Type: RSA
Key-Length: 4096
Subkey-Type: RSA
Subkey-Length: 4096
Name-Real: $REAL_NAME
Name-Email: $EMAIL
Expire-Date: 0
Passphrase: $PASSPHRASE
%commit
%echo Generated.
EOF

# The key id is TAKEN FROM the generation output, NOT by scanning the keyring afterwards.
# Scanning the keyring and "taking the last one" silently picks the WRONG key when more than one
# key exists for the same email (for example when the script is run a second time): one key goes
# into the secret, a different one is uploaded to the keyserver, and the release only blows up
# later when Central cannot verify the signature. --status-fd removes exactly that ambiguity.
STATUS="$(mktemp)"
gpg --batch --status-fd 3 --gen-key "$PARAMS" 3>"$STATUS"
cleanup

FPR="$(awk '/KEY_CREATED/ {print $NF}' "$STATUS" | tail -1)"
rm -f "$STATUS"
[[ -n "$FPR" ]] || { echo "ERROR: could not read the fingerprint of the generated key" >&2; exit 1; }
KEY_ID="$FPR"

echo
echo "Key id: $KEY_ID"
echo

echo "Uploading the public key to $KEYSERVER..."
if gpg --keyserver "$KEYSERVER" --send-keys "$KEY_ID"; then
    echo "  uploaded."
else
    echo "  WARNING: upload failed. Try it manually:" >&2
    echo "    gpg --keyserver $KEYSERVER --send-keys $KEY_ID" >&2
    echo "  Central verifies signatures against a keyserver; without this step the release is REJECTED." >&2
fi

# Private key: the passphrase is piped to gpg, NOT passed as an argument (so it is not visible in ps).
printf '%s' "$PASSPHRASE" \
  | gpg --batch --yes --pinentry-mode loopback --passphrase-fd 0 \
        --armor --export-secret-keys "$KEY_ID" > "$KEY_FILE"

echo
echo "Private key written to: $KEY_FILE"
echo
echo "Next steps — GitHub > Settings > Secrets and variables > Actions:"
echo "  GPG_PRIVATE_KEY  = the ENTIRE contents of $KEY_FILE (including the BEGIN/END lines)"
echo "  GPG_PASSPHRASE   = the passphrase you just entered"
echo
echo "AFTER adding the secrets, delete the file:"
echo "  shred -u $KEY_FILE   # or, if unavailable: rm -f $KEY_FILE"
