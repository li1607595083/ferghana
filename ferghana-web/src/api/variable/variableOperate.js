import request from '@/utils/request'

// 查询变量管理中心列表
export function listOperate(query) {
  return request({
    url: '/variable/operate/list',
    method: 'get',
    params: query
  })
}

// 新增变量管理中心
export function addOperate(data) {
  return request({
    url: '/variable/operate',
    method: 'post',
    data: data
  })
}

