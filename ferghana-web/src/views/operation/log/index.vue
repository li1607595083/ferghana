<template>
  <div class="app-container">

    <el-tabs type="card" @tab-click="handleClick"  v-model="tabActiveName">
      <el-tab-pane label="Log List" name="first">
        <el-table :data="functionList" v-loading="loading">
          <el-table-column label="日志名称" align="center" prop="name" >
            <template slot-scope="scope">
              <el-button
                type="text"
                @click="getLogDetailByName(scope.row)"
              >{{ scope.row.name }}
              </el-button>
            </template>
          </el-table-column>
          <el-table-column label="大小（KB）" align="center" prop="size"/>
        </el-table>
      </el-tab-pane>
      <el-tab-pane label="Stdout" name="second"/>
    </el-tabs>
    <div style="padding-top: 10px">
      <el-input v-model="logMessage" type="textarea" size="mini" :autosize="{ minRows: 20, maxRows: 20}"/>
    </div>

  </div>
</template>

<script>
  import {
    getTaskManagerLog
  } from "@/api/operation/log/taskManagerLog.js";
  import axios from "axios";
  import {getToken} from '@/utils/auth'

    export default {
      name: "index",
      data() {
        return {
          // 遮罩层
          loading: true,
          // 总条数
          total: 0,
          // 日志结果
          logMessage : '',
          // tab标签展示哪一页,进来默认显示第一页
          tabActiveName: "first",
          // 自定义函数表格数据
          functionList: []
        }
      },
      created() {
        this.getList();
      },
      methods: {
        /** 查询运行中作业的详情 */
        getList() {
          this.loading = true;
          let formData = new FormData();
          formData.append("applicationId", this.$route.params.applicationId);
          formData.append("taskmanagerId", this.$route.params.taskmanagerId);
          getTaskManagerLog(formData).then(response => {
            this.functionList = response.rows;
            this.total = response.total;
            this.loading = false;
          });
        },
        // 查询对应日志
        getLogDetailByName(row) {
          let that = this;
          const baseUrl = process.env.VUE_APP_BASE_API;
          axios({
            method: 'get',
            url: baseUrl + '/taskdevelop/operation/logname',
            headers: {'Authorization': 'Bearer ' + getToken()},
            responseType: 'json',
            params: {
              applicationId: row.applicationId,
              taskmanagerId: row.taskmanagerId,
              name: row.name
            }
          }).then(function (resp) {
            that.logMessage = resp.data.msg;
          }).catch(resp => {
            console.log('请求失败：' + resp.status + ',' + resp.statusText);
          });
        },
        // 点击标签事件
        handleClick(tab, event) {
          if (tab.name==='second'){
            let that = this;
            const baseUrl = process.env.VUE_APP_BASE_API;
            axios({
              method: 'get',
              url: baseUrl + '/taskdevelop/operation/stdout',
              headers: {'Authorization': 'Bearer ' + getToken()},
              responseType: 'json',
              params: {
                applicationId: this.functionList[0].applicationId,
                taskmanagerId: this.functionList[0].taskmanagerId
              }
            }).then(function (resp) {
              that.logMessage = resp.data.msg;
            }).catch(resp => {
              console.log('请求失败：' + resp.status + ',' + resp.statusText);
            });
          }
        }
      }
    }
</script>

<style scoped>

</style>
