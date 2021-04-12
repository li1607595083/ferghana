import request from '@/utils/request'

// 查询变量包管理列表
export function listManager(query) {
  return request({
    url: '/variable/package-manager/list',
    method: 'get',
    params: query
  })
}

// 查询变量包版本管理列表
export function listVersionManager(query) {
  return request({
    url: '/variable/package-manager/versionList',
    method: 'get',
    params: query
  })
}

// 查询变量包管理详细
export function getManager(variablePackId) {
  return request({
    url: '/variable/package-manager/' + variablePackId,
    method: 'get'
  })
}

// 新增变量包管理
export function addManager(data) {
  return request({
    url: '/variable/package-manager',
    method: 'post',
    data: data
  })
}

// 修改变量包管理
export function updateManager(data) {
  return request({
    url: '/variable/package-manager',
    method: 'put',
    data: data
  })
}

// 启动变量包
export function startVariablePackage(data) {
  return request({
    url: '/variable/package-manager/start',
    method: 'put',
    data: data
  })
}

// 删除变量包管理
export function delManager(variablePackId) {
  return request({
    url: '/variable/package-manager/' + variablePackId,
    method: 'delete'
  })
}

// 导出变量包管理
export function exportManager(query) {
  return request({
    url: '/variable/package-manager/export',
    method: 'get',
    params: query
  })
}

export function stopPackRun(data) {
  return request({
    url: '/variable/package-manager/stop',
    method:'put',
    data:data,
  })
}

// 新增变量管理中心
export function testPackage(data) {
  return request({
    url: '/variable/package-manager/test',
    method: 'post',
    data: data
  })
}
