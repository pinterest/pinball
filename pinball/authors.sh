git ls-tree -r --name-only HEAD -- */*.py | while read f; do
  echo $f;
  git blame --line-porcelain HEAD $f | grep "^author " | sort| uniq -c| sort -nr
done
