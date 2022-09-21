#!/bin/bash
GIT=`which git`
hconnectRepo="/home/ubuntu/2022_ite4065_2016025332/project1"
localRepo="/home/ubuntu/cp2022/project1/submission"

if [ -z $1 ]
then
    echo "No parameter passed"
else
    echo "Parameter passed commit msg = $1"
fi

echo -e "\n============================run package.sh============================\n"
./package.sh

echo -e "\n============================cp package to hconnect============================\n"
cp "$localRepo/submission.tar.gz" "$hconnectRepo/submission.tar.gz"

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
