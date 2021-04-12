import request from '@/utils/request'

// 查询自定义函数列表
export function list(query) {
  return request({
    url: '/variable/funcdevelop/list',
    method: 'get',
    params: query
  })
}
// 查询自定义函数列表
export function listFunction(query) {
  return request({
    url: '/variable/funcdevelop/listFunction',
    method: 'get',
    params: query
  })
}

// 查询自定义函数详细
export function getFunction(selfFunctionId) {
  return request({
    url: '/variable/funcdevelop/' + selfFunctionId,
    method: 'get'
  })
}

// 新增自定义函数
export function addFunction(data) {
  return request({
    url: '/variable/funcdevelop',
    method: 'post',
    data: data
  })
}

// 修改自定义函数包含更新jar
export function updateFunction(data) {
  return request({
    url: '/variable/funcdevelop',
    method: 'put',
    data: data
  })
}

// 修改自定义函数不更新jar
export function updateFunctionNoJar(data) {
  return request({
    url: '/variable/funcdevelop/updateNoJar',
    method: 'put',
    data: data
  })
}

// 删除自定义函数
export function delFunction(selfFunctionId) {
  return request({
    url: '/variable/funcdevelop/' + selfFunctionId,
    method: 'delete'
  })
}

// 导出自定义函数
export function exportFunction(query) {
  return request({
    url: '/variable/funcdevelop/export',
    method: 'get',
    params: query
  })
}

// 上传
export function uploadJar(data) {
  return request({
    url: '/variable/funcdevelop/upload',
    method: 'post',
    data: data
  })
}
// 上传
export function checkJar(data) {
  return request({
    url: '/variable/funcdevelop/checkJar',
    method: 'post',
    data: data
  })
}

// 查询详细
export function getTSelfFunction(postId) {
  return request({
    url: '/variable/funcdevelop/' + postId,
    method: 'get'
  })
}

