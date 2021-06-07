<template>
  <div class="app-container">
    <el-form :model="queryParams" ref="queryForm" :inline="true" label-width="90px">
      <el-form-item label="预警名称" prop="warningName">
        <el-input v-model="queryParams.warningName" placeholder="请输入预警名称" clearable size="small"
          @keyup.enter.native="handleQuery" />
      </el-form-item>
      <el-form-item label="变量包名称" prop="variablePackageName">
        <el-input v-model="queryParams.variablePackageName" placeholder="请输入变量包名称" clearable size="small"
          @keyup.enter.native="handleQuery" />
      </el-form-item>
      <el-form-item label="预警内容" prop="variablePackageName">
        <el-input v-model="queryParams.warningContent" placeholder="请输入预警内容" clearable size="small"
          @keyup.enter.native="handleQuery" />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" icon="el-icon-search" size="mini" @click="handleQuery">搜索</el-button>
        <el-button icon="el-icon-refresh" size="mini" @click="resetQuery">重置</el-button>
      </el-form-item>
    </el-form>

    <el-table v-loading="loading" :data="warningLogList">
      <el-table-column label="预警名称" width="300" align="left" prop="warningName" />
      <el-table-column label="变量包名称" width="300" align="left" prop="variablePackageName" />
      <el-table-column label="预警方式" width="250" align="left" prop="warningNoticeType" :formatter="NoticeTypeFormat" />
      <el-table-column label="预警联系人" width="250" align="left" prop="warningNoticeUser" :formatter="NoticeUserFormat" />
      <el-table-column label="预警内容" align="center" prop="warningContent" />
      <el-table-column label="预警时间"  width="170" align="center" prop="warningLogTime" />
    </el-table>

    <pagination v-show="total>0" :total="total" :page.sync="queryParams.pageNum" :limit.sync="queryParams.pageSize"
      @pagination="getList" />
    </el-form>
  </div>
</template>

<script>
  import {
    listLog
  } from "@/api/warning/log.js";
  import {
    getNoticeUserList
  } from "@/api/warning/config.js";
  export default {
    name: "WarningLog",
    data() {
      return {
        // 遮罩层
        loading: true,
        // 总条数
        total: 0,
        // 预警配置表格数据
        warningLogList: [],
        // 用户列表
        noticeUserOptions: [],
        // 预警通知类型
        noticeTypeOptions: [],
        // 详情不展示确定按钮
        showComfirm: true,
        // 详情不可修改
        detailViem: false,
        // 查询参数
        queryParams: {
          pageNum: 1,
          pageSize: 10,
          warningName: undefined,
          variablePackageName: undefined,
          warningContent: undefined
        },
      };
    },
    created() {
      this.getList();
      this.getNoticeUserList();
      this.getDicts("warning_config_notice_type").then(response => {
        this.noticeTypeOptions = response.data;
      });
    },
    methods: {
      /** 查询预警配置列表 */
      getList() {
        this.loading = true;
        listLog(this.queryParams).then(response => {
          console.log(response)
          this.warningLogList = response.rows;
          this.warningLogList.map((item, index) => {
            item.warningNoticeType = JSON.parse(item.warningNoticeType);
            item.warningNoticeUser = JSON.parse(item.warningNoticeUser);
            item.warningContent = JSON.parse(item.warningContent);
          })
          this.total = response.total;
          this.loading = false;
        });
      },
      getNoticeUserList() {
        getNoticeUserList().then(response => {
          this.noticeUserOptions = response.rows;
        })
      },
      // 预警通知联系人
      NoticeUserFormat(row, column) {
        var users = "";
        this.noticeUserOptions.map((item, index) => {
          row.warningNoticeUser.map((value, key) => {
            if(item.userId === value){
              users = users + (users.length > 0 ? "、" : "" ) + item.userName;
            }
          })
        })
        return users;
      },
      NoticeTypeFormat(row, column) {
        var result = "";
        var data = row.warningNoticeType;
        row.warningNoticeType.map((item, index) => {
          result = result + (result.length > 0 ? "、" : "" ) + this.selectDictLabel(this.noticeTypeOptions, item)
        })
        return result;
      },
      // 取消按钮
      cancel() {
        this.open = false;
        this.reset();
      },
      /** 搜索按钮操作 */
      handleQuery() {
        this.queryParams.pageNum = 1;
        this.getList();
      }
    }
  };
</script>

<style scoped>
  .elementStyle>>>.el-form-item__content {
    margin-left: 10px !important;
  }
</style>
