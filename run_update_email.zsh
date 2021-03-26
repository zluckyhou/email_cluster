#!/bin/zsh

# 记录一下开始时间
echo `date` >> /Users/taptap/Documents/python_project/email_cluster/log &&
# 进入获取email程序所在目录
cd /Users/taptap/Documents/python_project/email_cluster &&
# 执行python脚本（注意前面要指定python运行环境/usr/bin/python，根据自己的情况改变）
/Users/taptap/anaconda3/bin/python3 get_email_imap.py >> /Users/taptap/Documents/python_project/email_cluster/log &&
# 运行完成
echo 'finish' >> /Users/taptap/Documents/python_project/email_cluster/log
