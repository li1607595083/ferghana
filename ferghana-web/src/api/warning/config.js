import request from '@/utils/request'

// 查询预警列表
export function listConfig(query) {
  return request({
    url: '/warning/config/list',
    method: 'get',
    params: query
  })
}

// 查询变量包列表
export function listManager(query) {
  return request({
    url: '/warning/config/variablepackagelist',
    method: 'get',
    params: query
  })
}

// 启用预警
export function warningRun(data) {
  return request({
    url: '/warning/config/run',
    method: 'put',
    data: data
  })
}

// 停用预警
export function warningStop(data) {
  return request({
    url: '/warning/config/stop',
    method: 'put',
    data: data
  })
}

// 获取预警通知联系人
export function getNoticeUserList() {
  return request({
    url: '/warning/config/getuserlist',
    method: 'get'
  })
}

// 添加预警规则
export function addWarningConfig(data) {
  return request({
    url: '/warning/config/add',
    method: 'post',
    data: data
  })
}

// 查询预警详情
export function detailWarningConfig(warningId) {
  return request({
    url: '/warning/config/' + warningId,
    method: 'get'
  })
}

// 查询预警详情
export function updateWarningConfig(data) {
  return request({
    url: '/warning/config/update',
    method: 'post',
    data: data
  })
}

// 删除预警规则
export function delWarningConfig(warningIds) {
  return request({
    url: '/warning/config/' + warningIds,
    method: 'delete'
  })
}

function dateFormat(fmt, date) {
  let ret;
  const opt = {
      "Y+": date.getFullYear().toString(),        // 年
      "m+": (date.getMonth() + 1).toString(),     // 月
      "d+": date.getDate().toString(),            // 日
      "H+": date.getHours().toString(),           // 时
      "M+": date.getMinutes().toString(),         // 分
      "S+": date.getSeconds().toString()          // 秒
      // 有其他格式化字符需求可以继续添加，必须转化成字符串
  };
  for (let k in opt) {
      ret = new RegExp("(" + k + ")").exec(fmt);
      if (ret) {
          fmt = fmt.replace(ret[1], (ret[1].length == 1) ? (opt[k]) : (opt[k].padStart(ret[1].length, "0")))
      };
  };
  return fmt;
}

// 归置时间范围
export function getTimeRange(time, method){
  var curTime = new Date("1979/1/1 "+time);
  var newTime = null;
  if(method === "min")
    newTime = curTime.setHours(curTime.getHours() - 1);
  else if(method === "max")
    newTime = curTime.setHours(curTime.getHours() + 1)
  return dateFormat("HH:MM", new Date(newTime));
}
