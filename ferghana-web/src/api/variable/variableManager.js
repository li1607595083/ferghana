import request from '@/utils/request'

// 查询变量管理中心列表
export function listCenter(query) {
  return request({
    url: '/variable/manager/list',
    method: 'get',
    params: query
  })
}

export function listVersionCenter(query) {
  return request({
    url: '/variable/manager/listVersion',
    method: 'get',
    params: query
  })
}

// 查询变量管理中心详细
export function getCenter(variableId) {
  return request({
    url: '/variable/manager/' + variableId,
    method: 'get'
  })
}

// 新增变量管理中心
export function addCenter(data) {
  return request({
    url: '/variable/manager',
    method: 'post',
    data: data
  })
}

// 修改变量管理中心
export function updateCenter(data) {
  return request({
    url: '/variable/manager',
    method: 'put',
    data: data
  })
}

// 删除变量管理中心
export function delCenter(variableId) {
  return request({
    url: '/variable/manager/' + variableId,
    method: 'delete'
  })
}

// 导出变量管理中心
export function exportCenter(query) {
  return request({
    url: '/variable/manager/export',
    method: 'get',
    params: query
  })
}

// 新增变量管理中心
export function testVariable(data) {
  return request({
    url: '/variable/manager/test',
    method: 'post',
    data: data
  })
}
