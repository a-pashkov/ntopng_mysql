#!/bin/sh

cd ${0%${0##*/}}.

su -c "./rel/ntopng_mysql/bin/ntopng_mysql start" admin
