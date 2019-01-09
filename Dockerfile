FROM alpine:3.8

RUN apk update \
    && apk add \
      tini \
      ruby \
      ruby-bigdecimal \
      ruby-bundler \
      ruby-json \
      ruby-webrick \
    && echo 'gem: --no-rdoc --no-ri' > ~/.gemrc

RUN mkdir /usr/app
RUN adduser -h /usr/app -D worker

COPY Gemfile /usr/app/
COPY Gemfile.lock /usr/app/

WORKDIR /usr/app

RUN bundle install --without test

COPY bin /usr/app/bin
COPY lib /usr/app/lib

USER worker
ENTRYPOINT ["/sbin/tini", "--", "./bin/athenai"]
CMD ["save-history"]
