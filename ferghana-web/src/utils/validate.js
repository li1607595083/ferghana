/**
 * @param {string} path
 * @returns {Boolean}
 */
export function isExternal(path) {
  return /^(https?:|mailto:|tel:)/.test(path)
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validUsername(str) {
  const valid_map = ['admin', 'editor']
  return valid_map.indexOf(str.trim()) >= 0
}

/**
 * @param {string} url
 * @returns {Boolean}
 */
export function validURL(url) {
  const reg = /^(https?|ftp):\/\/([a-zA-Z0-9.-]+(:[a-zA-Z0-9.&%$-]+)*@)*((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]?)(\.(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])){3}|([a-zA-Z0-9-]+\.)*[a-zA-Z0-9-]+\.(com|edu|gov|int|mil|net|org|biz|arpa|info|name|pro|aero|coop|museum|[a-zA-Z]{2}))(:[0-9]+)*(\/($|[a-zA-Z0-9.,?'\\+&%$#=~_-]+))*$/
  return reg.test(url)
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validLowerCase(str) {
  const reg = /^[a-z]+$/
  return reg.test(str)
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validUpperCase(str) {
  const reg = /^[A-Z]+$/
  return reg.test(str)
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function validAlphabets(str) {
  const reg = /^[A-Za-z]+$/
  return reg.test(str)
}

/**
 * @param {string} email
 * @returns {Boolean}
 */
export function validEmail(email) {
  const reg = /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/
  return reg.test(email)
}

/**
 * @param {string} str
 * @returns {Boolean}
 */
export function isString(str) {
  if (typeof str === 'string' || str instanceof String) {
    return true
  }
  return false
}

/**
 * @param {Array} arg
 * @returns {Boolean}
 */
export function isArray(arg) {
  if (typeof Array.isArray === 'undefined') {
    return Object.prototype.toString.call(arg) === '[object Array]'
  }
  return Array.isArray(arg)
}

// 校验地址+端口号  规则：192.1.1.1:8080
export function isAddressPort(rule, value, callback) {
  const reg = /^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\:([0-9]|[1-9]\d{1,3}|[1-5]\d{4}|6[0-5]{2}[0-3][0-5])$/;
  if ((!reg.test(value)) && value != '') {
    callback(new Error('请输入正确的IP地址'));
  } else {
    callback();
  }
}

// 校验表的英文名 规则：英文字母、数字、下划线且首字母必须是英文字母 ^[a-zA-Z][a-zA-Z0-9_]*$
export function isLegitimateName(rule, value, callback) {
  const reg = /^[a-zA-Z][a-zA-Z0-9_]*$/;
  if ((!reg.test(value)) && value != '') {
    callback(new Error('请输入正确的英文名'));
  } else {
    callback();
  }
}

// 校验表的英文名 规则：英文字母、数字、下划线且首字母必须是英文字母 ^[a-zA-Z][a-zA-Z0-9_]*$
export function isHbaseName(rule, value, callback) {
  const reg = /^[a-zA-Z]*:[a-zA-Z0-9_]*$/;
  if ((!reg.test(value)) && value != '') {
    callback(new Error('请输入正确的英文名'));
  } else {
    callback();
  }
}
// 校验表的英文名 规则：英文字母、数字、下划线且首字母必须是英文字母 ^[a-zA-Z][a-zA-Z0-9_]*$
export function unContainSpace(rule, value, callback) {
  const reg =  /^[^ ]+$/;
  if ((!reg.test(value)) && value != '') {
    callback(new Error('不能包含空格'));
  } else {
    callback();
  }
}











