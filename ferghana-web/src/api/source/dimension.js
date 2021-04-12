import request from '@/utils/request'

// 查询数据维列表
export function listDimension(query) {
  return request({
    url: '/system/Dimension/list',
    method: 'get',
    params: query
  })
}

// 查询数据维详细
export function getDimension(dimensionId) {
  return request({
    url: '/system/Dimension/' + dimensionId,
    method: 'get'
  })
}

// 新增数据维
export function addDimension(data) {
  return request({
    url: '/system/Dimension',
    method: 'post',
    data: data
  })
}

// 修改数据维
export function updateDimension(data) {
  return request({
    url: '/system/Dimension',
    method: 'put',
    data: data
  })
}

// 删除数据维
export function delDimension(dimensionId) {
  return request({
    url: '/system/Dimension/' + dimensionId,
    method: 'delete'
  })
}

// 导出数据维
export function exportDimension(query) {
  return request({
    url: '/system/Dimension/export',
    method: 'get',
    params: query
  })
}
