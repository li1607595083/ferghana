import request from '@/utils/request'
// 查询详细
export function getOperations() {
  return request({
    url: '/taskdevelop/operation/',
    method: 'get'
  })
}
