email_1w.csv是1w个样本的数据集，完整的1.5G的数据集是email.csv。
processor文件是要在bash里面运行的，实验代码也是。
所有文件打包在trace里面


先运行yaml文件，然后容器里挂载trace文件夹。
docker cp trace/. sawtooth-shell-default:/project
进入bash
docker exec -it sawtooth-shell-default bash
启动processor
python3 /project/trace_data_processor.py
新开终端，进入bash启动experiment_runner.py,正常来说，实验数据logs也在bash里面。
python3 /project/experiment_runner.py
新开终端把容器里的logs拷贝到本地logs_host：
docker cp sawtooth-shell-default:/logs ./logs_host

experiment_runner.py最后面可以修改trace的样本数量。取100,500,1000,2000,1500,2500,3000
修改数量实验的时候，两个运行代码的bash不用关，直接在新终端重新拷贝trace到project，然后运行experiment，然后copy logs到logshost
