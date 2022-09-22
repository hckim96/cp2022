g++ prime_mt_bug.cpp -o prime_mt_bug -lpthread
g++ prime_cond.cpp -o prime_cond -lpthread

file1="result_prime_mt_bug.txt"
file2="result_prime_cond.txt"

./prime_mt_bug < workload.txt > result_prime_mt_bug.txt
./prime_cond < workload.txt > result_prime_cond.txt

if cmp -s "$file1" "$file2"; then
    echo 'The file "%s" is the same as "%s"\n' "$file1" "$file2"
else
    echo 'The file "%s" is different from "%s"\n' "$file1" "$file2"
fi