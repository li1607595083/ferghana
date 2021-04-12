<template>
  <div class="app-container">

    <el-table :data="functionList" v-loading="loading">
      <el-table-column label="作业名称" align="center" prop="name">
        <template slot-scope="scope">
          <router-link :to="'/operation/detail/'+ scope.row.applicationId + '/' + scope.row.jobId"
                       class="link-type">
            <span>{{ scope.row.name }}</span>
          </router-link>
        </template>
      </el-table-column>
      <el-table-column label="开始时间" align="center" prop="startTime">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.startTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="结束时间" align="center" prop="endTime">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.endTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="运行时长" align="center" prop="duration"/>
      <el-table-column label="状态" align="center" prop="state"/>
    </el-table>

    <pagination
      v-show="total>0"
      :total="total"
      :page.sync="queryParams.pageNum"
      :limit.sync="queryParams.pageSize"
      @pagination="getList"
    />
  </div>
</template>

<script>
  import {
    getOperations
  } from "@/api/operation/operation.js";

  export default {
    name: "index",
    data() {
      return {
        // 遮罩层
        loading: true,
        // 总条数
        total: 0,
        // 自定义函数表格数据
        functionList: [],
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

      /** 查询运行中作业的列表 */
      getList() {
        this.loading = true;
        getOperations().then(response => {
          this.loading = false;
          this.functionList = response.rows;
          this.total = response.total;
          this.loading = false;
        });
      },
      /** 详情按钮操作 */
      handleDetail(row) {
        console.log(row);
        this.$router.push("/operation/detail")
      }
    }
  }


</script>

<style scoped>

</style>
