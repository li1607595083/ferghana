import request from '@/utils/request'

// 查询预警列表
export function listLog(query) {
  return request({
    url: '/warning/log/list',
    method: 'get',
    params: query
  })
}
