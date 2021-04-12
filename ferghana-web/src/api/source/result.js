import request from '@/utils/request'
// 查询【请填写功能名称】列表
export function listResult(query) {
  return request({
    url: '/source/result/list',
    method: 'get',
    params: query
  })
}
// 修改【请填写功能名称】
export function updateSource(data) {
  return request({
    url: '/source/result',
    method: 'put',
    data: data
  })
}

// 新增【请填写功能名称】
export function addSource(data) {
  return request({
    url: '/source/result',
    method: 'post',
    data: data
  })
}

// 查询【请填写功能名称】详细
export function getSource(dataResultSourceId) {
  return request({
    url: '/source/result/' + dataResultSourceId,
    method: 'get'
  })
}


// 删除【请填写功能名称】
export function delSource(dataResultSourceId) {
  return request({
    url: '/source/result/' + dataResultSourceId,
    method: 'delete'
  })
}

// 导出【请填写功能名称】
export function exportSource(query) {
  return request({
    url: '/source/result/export',
    method: 'get',
    params: query
  })
}
