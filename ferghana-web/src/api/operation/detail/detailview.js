import request from '@/utils/request'
// 查询详细
export function getOperationDetail(data) {
  return request({
    url: '/taskdevelop/operation/detail',
    method: 'post',
    data: data
  })
}


