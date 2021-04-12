import request from '@/utils/request'

// 查询变量管理中心列表
export function listPackageOperate(data) {
  return request({
    url: '/variablePackage/operate/list/' + data,
    method: 'get',
  })
}

// 新增变量管理中心
export function addPackageOperate(data) {
  return request({
    url: '/variablePackage/operate',
    method: 'post',
    data: data
  })
}

