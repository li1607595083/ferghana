import request from '@/utils/request'
// 查询数据源列表
export function listSource(query) {
  return request({
    url: '/source/manage/list',
    method: 'get',
    params: query
  })
}
// 修改【请填写功能名称】
export function updateSource(data) {
  return request({
    url: '/source/manage',
    method: 'put',
    data: data
  })
}

// 新增【请填写功能名称】
export function addSource(data) {
  return request({
    url: '/source/manage',
    method: 'post',
    data: data
  })
}

// 查询数据源详细
export function getSource(postId) {
  return request({
    url: '/source/manage/' + postId,
    method: 'get'
  })
}



// 删除【请填写功能名称】
export function delSource(dataSourceId) {
  return request({
    url: '/source/manage/' + dataSourceId,
    method: 'delete'
  })
}

// 导出数据源
export function exportSource(query) {
  return request({
    url: '/source/manage/export',
    method: 'get',
    params: query
  })
}
