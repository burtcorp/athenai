FROM alpine:3.8

RUN apk update && apk add \
      ruby \
      ruby-bigdecimal \
      ruby-bundler \
      ruby-json \
      ruby-webrick

RUN mkdir /usr/app
WORKDIR /usr/app

COPY Gemfile /usr/app/
COPY Gemfile.lock /usr/app/

RUN bundle install --without test

COPY . /usr/app/

ENTRYPOINT ["/usr/bin/ruby"]
CMD ["bin/athenai"]
