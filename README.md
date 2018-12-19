# 基于jstorm-kafka的改造

*1.增加自动获取topic partition的功能
*2.修复offset数据自动过期的程序空转的问题
*3.增加不带ack时数据偏移量控制逻辑,避免offset队列内存溢出问题
*4.线程安全的offset队列,和程序down时的逻辑控制避免数据重复消费和丢失
