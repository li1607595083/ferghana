<template>
  <div class="app-container">

    <el-table :data="functionList" v-loading="loading">
      <el-table-column label="物理逻辑" align="center" width="500" prop="name" fixed>
        <!--        <template slot-scope="scope">-->
        <!--          <router-link :to="{ name: 'vertices',-->
        <!--           params: { applicationId: scope.row.applicationId,jobId:scope.row.jobId,verticesId: scope.row.verticesId }}"-->
        <!--                       class="link-type">-->
        <!--            <span>{{ scope.row.name }}</span>-->
        <!--          </router-link>-->
        <!--        </template>-->
      </el-table-column>
      <el-table-column label="状态" align="center" width="100" prop="status"/>
      <el-table-column label="接受字节" align="center" prop="bytes_received"/>
      <el-table-column label="接受记录" align="center" prop="records_received"/>
      <el-table-column label="发送字节" align="center" prop="bytes_send"/>
      <el-table-column label="发送记录" align="center" prop="records_send"/>
      <el-table-column label="并发数" align="center" prop="parallelism"/>
      <el-table-column label="开始时间" align="center" prop="startTime" width="200">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.startTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="结束时间" align="center" prop="endTime"/>
      <el-table-column label="运行时长" align="center" prop="duration" width="200"/>
      <el-table-column label="任务数" align="center" prop="tasks"/>
      <el-table-column label="操作" align="center" class-name="small-padding fixed-width" fixed="right">
        <template slot-scope="scope">
          <el-button
            size="mini"
            type="text"
            icon="el-icon-view"
            @click="handleDetail(scope.row)"
          >详情
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <!--    <pagination-->
    <!--      v-show="total>0"-->
    <!--      :total="total"-->
    <!--      :page.sync="queryParams.pageNum"-->
    <!--      :limit.sync="queryParams.pageSize"-->
    <!--      @pagination="getList"-->
    <!--    />-->

    <div style="padding-top: 30px" v-show="showDiv">
      <el-tabs v-model="activeName" type="card" @tab-click="handleClick">
        <el-tab-pane label="子任务" name="first">
          <el-table :data="subTaskList">
            <el-table-column label="id" align="center" prop="subtaskId"/>
            <el-table-column label="接收字节" align="center" prop="readBytes"/>
            <el-table-column label="接收记录" align="center" prop="readRecords"/>
            <el-table-column label="发送字节" align="center" prop="writeBytes"/>
            <el-table-column label="发送记录" align="center" prop="writeRecords"/>
            <el-table-column label="攻击" align="center" prop="attempt"/>
            <el-table-column label="host" align="center" prop="host"/>
            <el-table-column label="开始时间" align="center" width="200" prop="subStartTime">
              <template slot-scope="scope">
                <span>{{ parseTime(scope.row.subStartTime) }}</span>
              </template>
            </el-table-column>
            <el-table-column label="运行时间" align="center" width="200" prop="duration"/>
            <el-table-column label="结束时间" align="center" width="200" prop="subEndTime"/>
            <el-table-column label="状态" align="center" prop="status"/>
          </el-table>

        </el-tab-pane>
        <el-tab-pane label="任务管理" name="second">
          <el-table :data="taskManagerList">
            <el-table-column label="host" align="center" prop="host" width="200" fixed/>
            <el-table-column label="LOG" align="center" prop="LOG" fixed>
              <template slot-scope="scope">
                <router-link :to="'/operation/log/'+ scope.row.applicationId + '/' + scope.row.taskmanagerId"
                           class="link-type">
                  <span>LOG</span>
                </router-link>
              </template>
            </el-table-column>
            <el-table-column label="接收字节" align="center" prop="readBytes"/>
            <el-table-column label="接收记录" align="center" prop="readRecords"/>
            <el-table-column label="发送字节" align="center" prop="writeBytes"/>
            <el-table-column label="发送记录" align="center" prop="writeRecords"/>
            <el-table-column label="开始时间" align="center" width="200" prop="startTime">
              <template slot-scope="scope">
                <span>{{ parseTime(scope.row.startTime) }}</span>
              </template>
            </el-table-column>
            <el-table-column label="运行时间" align="center" width="200" prop="duration"/>
            <el-table-column label="结束时间" align="center" width="200" prop="sndTime"/>
            <el-table-column label="状态" align="center" prop="runing"/>
          </el-table>
        </el-tab-pane>
        <el-tab-pane label="Watermarks" name="third">Watermarks</el-tab-pane>
        <el-tab-pane label="Accumulators" name="fourth">Accumulators</el-tab-pane>
      </el-tabs>
    </div>
  </div>


</template>

<script>
  import {
    getOperationDetail
  } from "@/api/operation/detail/detailview.js";
  import axios from 'axios'
  import {getToken} from '@/utils/auth'

  export default {
    name: "Detail",
    data() {
      return {
        // 遮罩层
        loading: true,
        // 总条数
        total: 0,
        // 自定义函数表格数据
        functionList: [],
        // 子任务结果集
        subTaskList: [],
        // 任务管理
        taskManagerList: [],
        //
        activeName: 'first',
        // 展示div
        showDiv: false,
        // 展示当前行数据
        rowData: "",
        // 查询参数
        queryParams: {
          pageNum: 1,
          pageSize: 10
        }
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
        formData.append("jobId", this.$route.params.jobId);
        getOperationDetail(formData).then(response => {
          this.functionList = response.rows;
          this.total = response.total;
          this.loading = false;
        });
      },

      // 详情 查询子任务
      handleDetail(row) {
        this.rowData = row;
        this.showDiv = true;
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API
        axios({
          method: 'get',
          url: baseUrl + '/taskdevelop/operation/vertices',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json',
          params: {
            applicationId: row.applicationId,
            jobId: row.jobId,
            verticesId: row.verticesId
          }
        }).then(function (resp) {
          that.subTaskList = resp.data.rows;
        }).catch(resp => {
          console.log('请求失败：' + resp.status + ',' + resp.statusText);
        });
      },

      // 详情 查询任务管理
      handleTaskManagers(row) {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API
        axios({
          method: 'get',
          url: baseUrl + '/taskdevelop/operation/taskManagers',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json',
          params: {
            applicationId: row.applicationId,
            jobId: row.jobId,
            verticesId: row.verticesId
          }
        }).then(function (resp) {
          that.taskManagerList = resp.data.rows;
        }).catch(resp => {
          console.log('请求失败：' + resp.status + ',' + resp.statusText);
        });
      },

      handleClick(tab, event) {
        console.log(tab, event);
        if (tab.name === 'first') { // 点击子任务列
          this.handleDetail(this.rowData);
        } else if (tab.name === 'second') {
          this.handleTaskManagers(this.rowData);
        }
      },
    }
  }
</script>

<style scoped>

</style>
