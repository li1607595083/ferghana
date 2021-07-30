import request from '@/utils/request'
// 查询列表数据
export function getInfo() {
  return request({
    url: '/taskdevelop/operation/info',
    method: 'get'
  })
}

// 查询图表数据
export function getChartInfo(jobId) {
  return request({
    url: `/taskdevelop/operation/chart-info/${jobId}`,
    method: 'get'
  })
}

// 查询已启动变量包列表
export function getStartInfo() {
  return request({
    url: `/taskdevelop/operation/start-list`,
    method: 'get'
  })
}


export function dateFormat(fmt, date) {
  let ret;
  const opt = {
    "Y+": date.getFullYear().toString(), // 年
    "m+": (date.getMonth() + 1).toString(), // 月
    "d+": date.getDate().toString(), // 日
    "H+": date.getHours().toString(), // 时
    "M+": date.getMinutes().toString(), // 分
    "S+": date.getSeconds().toString() // 秒
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
