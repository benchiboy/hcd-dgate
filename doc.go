/*
HCD-D document
*/
package main

/*
#ver 0.1
#2019-07-09
	在LINUX测试4000个客户端同时连接
	验证了DB 连接池 SETMAXLIFETIME 的作用，这个需要进一步验证
ver 0.2
#2019-07-11
	上线，下线只更新DEVICE_TIME
	下线也插入历史表
	修改得到渠道没插入主表的错误

987193711

*/

//-Send Command---> 204 ~^S^@^@^@<CC>{"method":"push_file_info","sn":"001D099BEDDA","chip_id":"BJ4233245","type":"up
//grade","total_file":1,"file_in_procesing":1,"file":{"name":"DMSV1.0.04k.zip","length":75457071,"file_crc":-1374992681}} <nil>
