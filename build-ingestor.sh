set -e
echo ">>> Pulling latest changes from the Git repository..."
git pull
echo ">>> Building the Go application..."
go build -o unity-alerts main.go
echo ">>> Build complete! Binary 'unity-alerts' is ready."