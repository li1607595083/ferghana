import request from '@/utils/request'

// 查询变量分类列表
export function listClassification(query) {
  return request({
    url: '/variable/classification/list',
    method: 'get',
    params: query
  })
}

// 查询变量分类详细
export function getClassification(variableClassificationId) {
  return request({
    url: '/variable/classification/' + variableClassificationId,
    method: 'get'
  })
}

// 新增变量分类
export function addClassification(data) {
  return request({
    url: '/variable/classification',
    method: 'post',
    data: data
  })
}

// 修改变量分类
export function updateClassification(data) {
  return request({
    url: '/variable/classification',
    method: 'put',
    data: data
  })
}

// 删除变量分类
export function delClassification(variableClassificationId) {
  return request({
    url: '/variable/classification/' + variableClassificationId,
    method: 'delete'
  })
}

// 导出变量分类
export function exportClassification(query) {
  return request({
    url: '/variable/classification/export',
    method: 'get',
    params: query
  })
}
