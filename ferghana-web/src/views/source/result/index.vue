<template>
  <div class="app-container">
    <el-form :model="queryParams" ref="queryForm" :inline="true" label-width="100px">
      <el-form-item label="结果表中文名" prop="dataSourceName">
        <el-input
          v-model="queryParams.dataSourceName"
          placeholder="请输入数据结果表名称"
          clearable
          size="small"
          @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item label="连接器类型" prop="connectorType" label-width="100px">
        <el-select
          v-model="queryParams.connectorType"
          placeholder="请选择连接器类型"
          clearable
          size="small"
        >
          <el-option
            v-for="dictSource in connectorTypeOptions"
            :key="dictSource.dictValue"
            :label="dictSource.dictLabel"
            :value="dictSource.dictValue"
          />
        </el-select>
      </el-form-item>
      <el-form-item label="描述" prop="description">
        <el-input v-model="queryParams.description" placeholder="请输入描述" clearable size="small"
                  @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" icon="el-icon-search" size="mini" @click="handleQuery">搜索</el-button>
        <el-button icon="el-icon-refresh" size="mini" @click="resetQuery">重置</el-button>
      </el-form-item>
    </el-form>

    <el-row :gutter="10" class="mb8">
      <el-col :span="1.5">
        <el-button
          type="primary"
          icon="el-icon-plus"
          size="mini"
          @click="handleAdd"
        >新增
        </el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button
          type="primary"
          icon="el-icon-edit"
          size="mini"
          :disabled="single"
          @click="handleUpdate"
        >修改
        </el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button
          type="primary"
          icon="el-icon-delete"
          size="mini"
          :disabled="multiple"
          @click="handleDelete"
        >删除
        </el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button
          type="primary"
          icon="el-icon-download"
          size="mini"
          @click="handleExport"
          v-hasPermi="['system:source:export']"
        >导出
        </el-button>
      </el-col>
    </el-row>

    <el-table v-loading="loading" :data="sourceList" @selection-change="handleSelectionChange">
      <el-table-column type="selection" width="55" align="center"/>
      <el-table-column label="序号" width="55" align="center" type="index"/>
      <el-table-column label="结果表中文名" align="center" prop="dataSourceName"/>
      <el-table-column label="结果表英文名" align="center" prop="tableName"/>
      <el-table-column
        label="连接器类型"
        align="center"
        prop="connectorType"
        :formatter="connectorTypeMatter"
      />
      <el-table-column label="创建时间" align="center" prop="createTime" width="160">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.createTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="修改时间" align="center" prop="modifyTime" width="160">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.modifyTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="操作" align="center" class-name="small-padding fixed-width">
        <template slot-scope="scope">
          <el-button
            size="mini"
            type="text"
            icon="el-icon-view"
            @click="handleDetail(scope.row)"
            v-hasPermi="['system:source:edit']"
          >详情
          </el-button>
          <el-button
            size="mini"
            type="text"
            icon="el-icon-edit"
            @click="handleUpdate(scope.row)"
            v-hasPermi="['system:source:edit']"
          >修改
          </el-button>
          <el-button
            size="mini"
            type="text"
            icon="el-icon-delete"
            @click="handleDelete(scope.row)"
            v-hasPermi="['system:source:remove']"
          >删除
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <pagination
      v-show="total>0"
      :total="total"
      :page.sync="queryParams.pageNum"
      :limit.sync="queryParams.pageSize"
      @pagination="getList"
    />

    <!-- 添加或修改【请填写功能名称】对话框 -->
    <el-dialog :title="title" :visible.sync="open" width="1050px" :close-on-click-modal="false">
      <el-form ref="form" :model="form" :rules="rules" label-width="130px" class="el-col-24">
        <el-form-item label="结果表中文名" prop="dataSourceName" class="el-col-12">
          <el-input v-model="form.dataSourceName" placeholder="请输入结果表中文名" :disabled="detailViem"/>
        </el-form-item>
        <el-form-item label="结果表英文名" prop="tableName" class="el-col-12">
          <el-input v-model="form.tableName" placeholder="请输入结果表英文名" :disabled="detailViem"/>
        </el-form-item>
        <el-form-item label="连接器类型" prop="connectorType" class="el-col-12">
          <el-select
            v-model="form.connectorType"
            placeholder="请选择连接器类型"
            clearable
            style="width: 100%"
            @change="connectorTypeChange"
            :disabled="detailViem"
          >
            <el-option
              v-for="dict in connectorTypeOptions"
              :key="dict.dictValue"
              :label="dict.dictLabel"
              :value="dict.dictValue"
            />
          </el-select>
        </el-form-item>
        <div v-show="kafkaDiv">
          <el-form-item label="topic名" prop="topicName" class="el-col-12">
            <el-input v-model="form.topicName" placeholder="请输入topic名" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="zookeeper地址" prop="zookeeperAddress" class="el-col-12">
            <el-input v-model="form.zookeeperAddress" placeholder="master:2181,slave:2181" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="kafka地址" prop="kafkaAddress" class="el-col-12">
            <el-input v-model="form.kafkaAddress" placeholder="master:9092,slave:9092 " :disabled="detailViem"/>
          </el-form-item>
        </div>

        <!--        jdbc -->
        <div v-show="jdbcItem">
          <el-form-item label="URL地址" prop="jdbcUrlAddress" class="el-col-12">
            <el-input v-model="form.jdbcUrlAddress" placeholder="请输入URL地址" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="驱动类" prop="jdbcDrive" class="el-col-12">
            <el-input v-model="form.jdbcDrive" placeholder="请输入驱动类" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="用户名" prop="jdbcUserName" class="el-col-12">
            <el-input v-model="form.jdbcUserName" placeholder="请输入用户名" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="密码" prop="jdbcUserPwd" class="el-col-12">
            <el-input v-model="form.jdbcUserPwd" placeholder="请输入密码" :disabled="detailViem"/>
          </el-form-item>
        </div>

        <div v-show="hbaseItem">
          <el-form-item label="ZK地址" prop="hbaseZKAddress" class="el-col-12">
            <el-input v-model="form.hbaseZKAddress" placeholder="master:2181,slave:2181" :disabled="detailViem"/>
          </el-form-item>
        </div>

        <div v-show="esItem">
          <el-form-item label="ES地址" prop="esAddress" class="el-col-12">
            <el-input v-model="form.esAddress" placeholder="master:9200,slave:9200" :disabled="detailViem"/>
          </el-form-item>
        </div>

        <el-form-item label="描述" prop="description" class="el-col-24">
          <el-input v-model="form.description" placeholder="请输入描述" type="textarea" :disabled="detailViem"/>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer cl-col-24">
        <el-button type="primary" @click="submitForm" v-show="showSubmitForm">确 定</el-button>
        <el-button @click="cancel">取 消</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
  import {listResult, getSource, delSource, addSource, updateSource, exportSource} from "@/api/source/result.js";
  import{isAddressPort,isLegitimateName,unContainSpace} from "@/utils/validate.js";

  export default {
    name: "Result",
    data() {
      return {
        // 遮罩层
        loading: true,
        // 选中数组
        ids: [],
        // 非单个禁用
        single: true,
        // 非多个禁用
        multiple: true,
        // 总条数
        total: 0,
        // 【请填写功能名称】表格数据
        sourceList: [],
        // 弹出层标题
        title: "",
        // 是否显示弹出层
        open: false,
        // 数据源类型数据
        dataSourceTypeOptions: [],
        // 连接器类型
        connectorTypeOptions: [],

        // 消费模式类型
        consumerModeOptions: [],
        // 数据类型
        sysDataBaseTypes: [],
        // 控制标签是否可修改
        detailViem: false,
        // 控制【确认】按钮是否显示
        showSubmitForm: true,
        // 控制kafka显示参数
        kafkaDiv: true,
        // 控制jdbc显示参数
        jdbcItem: false,
        // 控制hbase显示参数
        hbaseItem: false,
        // 控制ES显示参数
        esItem: false,
        // 查询参数
        queryParams: {
          pageNum: 1,
          pageSize: 10,
          dataSourceName: undefined,
          dataSourceType: undefined,
          connectorType: undefined,
          dataSource: undefined,
          topicName: undefined,
          tableName: undefined,
          consumerGroup: undefined,
          consumerMode: undefined,
          zookeeperAddress: undefined,
          kafkaAddress: undefined,
          schemaDefine: undefined,
          dataBaseType: undefined,
          description: undefined,
          modifyTime: undefined
        },
        // 表单参数
        form: {},
        // 表单校验
        rules: {
          dataSourceName: [
            {required: true, message: "结果表中文名不能为空", trigger: "blur"},
            {validator: unContainSpace, message: "结果表中文名不能包含空格", trigger: "blur"}
          ],
          dataSourceType: [
            {required: true, message: "数据源类型不能为空", trigger: "blur"}
          ],
          connectorType: [
            {required: true, message: "连接器类型不能为空", trigger: "blur"}
          ],
          dataSource: [
            {required: true, message: "数据来源不能为空", trigger: "blur"}
          ],
          topicName: [
            {required: true, message: "topic名不能为空", trigger: "blur"}
          ],
          tableName: [
            {required: true, message: "结果表英文名不能为空", trigger: "blur"},
            {validator: isLegitimateName, message: "格式：只能包含英文字母、数字、下划线且首字母必须是英文字母", trigger: "blur"}
          ],
          consumerGroup: [
            {required: true, message: "消费组不能为空", trigger: "blur"}
          ],
          consumerMode: [
            {required: true, message: "消费模式不能为空", trigger: "blur"}
          ],
          zookeeperAddress: [
            {required: true, message: "zookeeper地址不能为空", trigger: "blur"}
          ],
          kafkaAddress: [
            {required: true, message: "kafka地址不能为空", trigger: "blur"}
          ],
          jdbcUrlAddress: [
            {required: true, message: "地址不能为空", trigger: "blur"}
          ],
          jdbcDrive: [
            {required: true, message: "驱动类不能为空", trigger: "blur"}
          ],
          jdbcUserName: [
            {required: true, message: "用户名不能为空", trigger: "blur"}
          ],
          jdbcUserPwd: [
            {required: true, message: "密码不能为空", trigger: "blur"}
          ],
          hbaseZKAddress: [
            {required: true, message: "zk地址不能为空", trigger: "blur"}
          ],
          esAddress: [
            {required: true, message: "ES地址不能为空", trigger: "blur"},
          ],
        }
      };
    },
    created() {
      this.getList();
      this.getDicts("sys_source_type").then(response => {
        this.dataSourceTypeOptions = response.data;
      });
      this.getDicts("data_result_conn_type").then(response => {
        this.connectorTypeOptions = response.data;
      });
      this.getDicts("sys_consumer_mode").then(response => {
        this.consumerModeOptions = response.data;
      });
      this.getDicts("sys_data_base_type").then(response => {
        this.sysDataBaseTypes = response.data;
      });
    },
    methods: {

      // 连接器类型切换
      connectorTypeChange(value) {
        console.log("------connectorTypeChange");
        console.log(value);
        if ("01" === value) { // kafka
          this.kafkaCheck();
        } else if ("02" === value) { // jdbc
          this.jdbcCheck();
        } else if ("03" === value) { // hbase
          this.hbaseCheck();
        } else if ("04" === value) { // ES
          this.esCheck();
        }
      },
      kafkaCheck() {
        this.kafkaDiv = true;
        this.jdbcItem = false;
        this.hbaseItem = false;
        this.esItem = false;
        this.rules.topicName[0].required = true;
        this.rules.zookeeperAddress[0].required = true;
        this.rules.kafkaAddress[0].required = true;
        this.rules.jdbcUrlAddress[0].required = false;
        this.rules.jdbcDrive[0].required = false;
        this.rules.jdbcUserName[0].required = false;
        this.rules.jdbcUserPwd[0].required = false;
        this.rules.hbaseZKAddress[0].required = false;
        this.rules.esAddress[0].required = false;
      },
      jdbcCheck() {
        this.kafkaDiv = false;
        this.jdbcItem = true;
        this.hbaseItem = false;
        this.esItem = false;
        this.rules.topicName[0].required = false;
        this.rules.zookeeperAddress[0].required = false;
        this.rules.kafkaAddress[0].required = false;
        this.rules.jdbcUrlAddress[0].required = true;
        this.rules.jdbcDrive[0].required = true;
        this.rules.jdbcUserName[0].required = true;
        this.rules.jdbcUserPwd[0].required = true;
        this.rules.hbaseZKAddress[0].required = false;
        this.rules.esAddress[0].required = false;
      },
      hbaseCheck() {
        this.kafkaDiv = false;
        this.jdbcItem = false;
        this.hbaseItem = true;
        this.esItem = false;
        this.rules.topicName[0].required = false;
        this.rules.zookeeperAddress[0].required = false;
        this.rules.kafkaAddress[0].required = false;
        this.rules.jdbcUrlAddress[0].required = false;
        this.rules.jdbcDrive[0].required = false;
        this.rules.jdbcUserName[0].required = false;
        this.rules.jdbcUserPwd[0].required = false;
        this.rules.hbaseZKAddress[0].required = true;
        this.rules.esAddress[0].required = false;
      },
      esCheck() {
        this.kafkaDiv = false;
        this.jdbcItem = false;
        this.hbaseItem = false;
        this.esItem = true;
        this.rules.topicName[0].required = false;
        this.rules.zookeeperAddress[0].required = false;
        this.rules.kafkaAddress[0].required = false;
        this.rules.jdbcUrlAddress[0].required = false;
        this.rules.jdbcDrive[0].required = false;
        this.rules.jdbcUserName[0].required = false;
        this.rules.jdbcUserPwd[0].required = false;
        this.rules.hbaseZKAddress[0].required = false;
        this.rules.esAddress[0].required = true;
      },
      // 数据源类型字典翻译
      statusDataSourceType(row, column) {
        return this.selectDictLabel(
          this.dataSourceTypeOptions,
          row.dataSourceType
        );
      },
      // 连接器类型字典翻译
      connectorTypeMatter(row, column) {
        return this.selectDictLabel(this.connectorTypeOptions, row.connectorType);
      },
      /** 查询【请填写功能名称】列表 */
      getList() {
        this.loading = true;
        listResult(this.queryParams).then(response => {
          this.sourceList = response.rows;
          this.total = response.total;
          this.loading = false;
        });
      },
      // 取消按钮
      cancel() {
        this.open = false;
        this.reset();
      },
      // 表单重置
      reset() {
        this.form = {
          dataResultSourceId: undefined,
          dataSourceName: undefined,
          dataSourceType: undefined,
          connectorType: undefined,
          dataSource: undefined,
          topicName: undefined,
          tableName: undefined,
          consumerGroup: undefined,
          consumerMode: undefined,
          zookeeperAddress: undefined,
          kafkaAddress: undefined,
          schemaDefine: undefined,
          dataBaseType: undefined,
          description: undefined,
          jdbcUrlAddress: undefined,
          jdbcDrive: undefined,
          jdbcUserName: undefined,
          jdbcUserPwd: undefined,
          hbaseZKAddress: undefined,

          createTime: undefined,
          modifyTime: undefined,
        };
        this.resetForm("form");
        this.form.connectorType = "01";
      },
      /** 搜索按钮操作 */
      handleQuery() {
        this.queryParams.pageNum = 1;
        this.getList();
      },
      /** 重置按钮操作 */
      resetQuery() {
        this.resetForm("queryForm");
        this.handleQuery();
      },
      // 多选框选中数据
      handleSelectionChange(selection) {
        this.ids = selection.map(item => item.dataResultSourceId)
        this.single = selection.length !== 1
        this.multiple = !selection.length
      },
      /** 新增按钮操作 */
      handleAdd() {
        this.reset();
        this.detailViem = false;
        this.showSubmitForm = true;
        this.open = true;
        this.title = "添加数据结果表";
        this.form.connectorType = "01";
        this.connectorTypeChange("01");
      },
      /** 修改按钮操作 */
      handleUpdate(row) {
        this.reset();
        const dataResultSourceId = row.dataResultSourceId || this.ids
        getSource(dataResultSourceId).then(response => {
          this.form = response.data;
          this.open = true;
          this.detailViem = false;
          this.showSubmitForm = true;
          this.title = "修改数据结果表";
          this.connectorTypeChange(this.form.connectorType);
        });
      },
      /** 详情按钮操作 */
      handleDetail(row) {
        this.reset();
        const dataResultSourceId = row.dataResultSourceId || this.ids;
        getSource(dataResultSourceId).then(response => {
          this.form = response.data;
          this.open = true;
          this.detailViem = true;
          this.showSubmitForm = false;
          this.title = "查看数据结果表";
          this.connectorTypeChange(this.form.connectorType);
        });
      },
      /** 提交按钮 */
      submitForm: function () {
        this.$refs["form"].validate(valid => {
          if (valid) {
            if (this.form.dataResultSourceId !== undefined) {
              updateSource(this.form).then(response => {
                if (response.code === 200) {
                  this.msgSuccess("修改成功");
                  this.open = false;
                  this.getList();
                } else {
                  this.msgError(response.msg);
                }
              });
            } else {
              addSource(this.form).then(response => {
                if (response.code === 200) {
                  this.msgSuccess("新增成功");
                  this.open = false;
                  this.getList();
                } else {
                  this.msgError(response.msg);
                }
              });
            }
          }
        });
      },
      /** 删除按钮操作 */
      handleDelete(row) {
        const dataResultSourceIds = row.dataResultSourceId || this.ids;
        this.$confirm('是否确认删除该数据结果表?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function () {
          return delSource(dataResultSourceIds);
        }).then(() => {
          this.getList();
          this.msgSuccess("删除成功");
        }).catch(function () {
        });
      },
      /** 导出按钮操作 */
      handleExport() {
        const queryParams = this.queryParams;
        this.$confirm('是否确认导出所有数据结果表?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function () {
          return exportSource(queryParams);
        }).then(response => {
          this.download(response.msg);
        }).catch(function () {
        });
      }
    }
  };
</script>

<style scoped>
  .elementStyle >>> .el-form-item__content {
    margin-left: 10px !important;
  }
</style>
