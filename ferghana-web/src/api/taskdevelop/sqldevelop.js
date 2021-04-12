import request from '@/utils/request'

// 查询SQL任务开发列表
export function listDevelop(query) {
  return request({
    url: '/taskdevelop/sqldevelop/list',
    method: 'get',
    params: query
  })
}

// 查询SQL任务开发详细
export function getDevelop(sqlDevelopId) {
  return request({
    url: '/taskdevelop/sqldevelop/' + sqlDevelopId,
    method: 'get'
  })
}

// 新增SQL任务开发
export function addDevelop(data) {
  return request({
    url: '/taskdevelop/sqldevelop',
    method: 'post',
    data: data
  })
}

// 修改SQL任务开发
export function updateDevelop(data) {
  return request({
    url: '/taskdevelop/sqldevelop',
    method: 'put',
    data: data
  })
}

// 升级SQL任务开发版本
export function insertDevelopHigh(data) {
  return request({
    url: '/taskdevelop/sqldevelop/developHigh',
    method: 'put',
    data: data
  })
}

// 删除SQL任务开发
export function delDevelop(sqlDevelopId) {
  return request({
    url: '/taskdevelop/sqldevelop/' + sqlDevelopId,
    method: 'delete'
  })
}

// 导出SQL任务开发
export function exportDevelop(query) {
  return request({
    url: '/taskdevelop/sqldevelop/export',
    method: 'get',
    params: query
  })
}

// SQL的语法校验
export function sqlCheck(sqlString) {
  let reg=new RegExp('%0A','g');//g代表全部
  return request({
    url: '/taskdevelop/sqldevelop/sqlCheck/' +encodeURIComponent(sqlString).replace(reg,'%20'),
    method: 'get'
  })
}

// SQL的调试
export function sqlTest(params) {
  return request({
    url: '/taskdevelop/sqldevelop/sqlRun',
    method: 'post',
    datatype: "json",
    contentType: "application/json; charset=utf-8",
    data: params
  })
}

// 作业的启动与停止
export function changeTaskStatus(sqlDevelopId, runStatus,sqlTaskName,sourceTableId,resultTableIds,sqlContent,applicationId) {
  const data = {
    sqlDevelopId,
    runStatus,
    sqlTaskName,
    sourceTableId,
    resultTableIds,
    sqlContent,
    applicationId
  };
  return request({
    url: '/taskdevelop/sqldevelop',
    method: 'put',
    data: data
  })
}

