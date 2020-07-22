CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o hcd-dgate-tmp


CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o gzh


scp  ./gzh root@182.254.198.93:/data/www/kygate/gzh

scp  ./html/index.html root@182.254.198.93:/data/www/kygate/gzh