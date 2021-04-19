CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o hcd-dgate-tmp


CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o gzh


scp  ./gzh root@182.254.198.93:/data/www/kygate/gzh

scp  ./html/index.html root@182.254.198.93:/data/www/kygate/gzh



scp  ./hcd-dgate-tmp  root@120.78.198.154:/data/app/hcd-dgate-tmp

scp  ./hcd-dgate-tmp  root@119.23.252.145:/data/app/hcd-dgate-tmp

