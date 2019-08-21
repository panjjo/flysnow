FROM harbor.yunss.com:5000/base/base:latest
ADD config.yaml /config.yaml
ADD srv /srv
ADD update /update
ENTRYPOINT [ "/srv" ]
