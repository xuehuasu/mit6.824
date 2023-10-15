#!/bin/bash  

FLAG=false
total_time=0
result() { 
    if [[ ${FLAG} == true ]]; then
        echo "FAIL"
    else
        echo "PASS ALL " ${i}
    fi
    echo "time: ${total_time}s"
    exit 1
}  
trap result SIGINT 

filename="out/allTestOutput.log"
rm filename
i=1
for ((; i<=3000; i++))  
do  
    s_time=$(date +%s)
    echo "Running test ${test} iteration $i..."  
    
    go test -race >> ${filename} &
    wait # 等待上一个命令结束 
    e_time=$(date +%s)
    run_time=$((e_time - s_time))
    total_time=$((total_time + run_time))

    # 检查日志最后一行的第一个单词是否为 "FAIL"
    last_line=$(tail -n 1 ${filename})
    first_word=$(echo "$last_line" | awk '{print $1}')
    second_word=$(echo "$last_line" | awk '{print $2}')
    if [[ "$first_word" == "FAIL" || second_word == "FAIL" ]]; then
        FLAG=true
        echo "--- FAIL "${i}" time: ${run_time}s"
    else
        echo "PASS "${i}" time: ${run_time}s"
    fi
done

result()
