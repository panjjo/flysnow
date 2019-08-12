FROM harbor.yunss.com:5000/base/base:latest
ADD config /config
ADD srv /srv
ENTRYPOINT [ "/srv" ]
