import request from '@/utils/request'
// 查询详细
export function getTaskManagerLog(data) {
  return request({
    url: '/taskdevelop/operation/logList',
    method: 'post',
    data: data
  })
}

