#!/usr/bin/env bash
tag=$(cat setup.py|grep version|head -n 1|awk -F"'" '{print $2}')
git tag ${tag}
git push origin ${tag}
