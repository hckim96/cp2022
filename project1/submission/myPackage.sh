#!/bin/bash
GIT=`which git`
hconnectRepo="/home/ubuntu/2022_ite4065_2016025332/project1"
localRepo="/home/ubuntu/cp2022/project1/submission"

if [ -z $1 ]
then
    echo "No parameter passed"
else
    echo "Parameter passed = $1"
fi
commitMsg=$1

# package

echo -e "\n============================run package.sh============================\n"
./package.sh

echo -e "\n============================local push============================\n"
cd $localRepo
${GIT} add .
${GIT} commit -m "$1"
${GIT} push

echo -e "\n============================hconnect push============================\n"
cd $hconnectRepo
${GIT} add "$hconnectRepo/submission.tar.gz"
${GIT} commit -m "$1"
${GIT} push
