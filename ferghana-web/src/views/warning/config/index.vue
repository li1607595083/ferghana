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
      <el-form-item label="状态" prop="warningState">
        <el-select v-model="queryParams.warningState" placeholder="请选择预警状态" clearable size="small">
          <el-option v-for="dict in warningStateOptions" :key="dict.dictValue" :label="dict.dictLabel"
            :value="dict.dictValue" />
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" icon="el-icon-search" size="mini" @click="handleQuery">搜索</el-button>
        <el-button icon="el-icon-refresh" size="mini" @click="resetQuery">重置</el-button>
      </el-form-item>
    </el-form>

    <el-row :gutter="10" class="mb8">
      <el-col :span="1.5">
        <el-button type="primary" icon="el-icon-plus" size="mini" @click="handleAdd" v-hasPermi="['warning:config:add']">新增
        </el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button type="primary" icon="el-icon-edit" size="mini" :disabled="single" @click="handleUpdate" v-hasPermi="['warning:config:edit']">修改
        </el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button type="primary" icon="el-icon-delete" size="mini" :disabled="multiple" @click="handleDelete" v-hasPermi="['warning:config:remove']">删除
        </el-button>
      </el-col>
    </el-row>

    <el-table v-loading="loading" :data="warningConfigList" @selection-change="handleSelectionChange"
      @row-dblclick="handleDetail">
      <el-table-column type="selection" width="45" align="center" />
      <el-table-column label="预警名称" align="left" prop="warningName" />
      <el-table-column label="变量包名称" align="left" prop="variablePackageName" />
      <el-table-column label="预警方式" align="left" prop="warningNoticeType" :formatter="NoticeTypeFormat" />
      <el-table-column label="状态" width="100" align="center" prop="warningState" :formatter="warningStateFormat">
        <template slot-scope="scope">
          <el-switch v-model="scope.row.warningState" active-value="1" inactive-value="0"
            @change="handleStatusChange(scope.row)" />
        </template>
      </el-table-column>
      <el-table-column label="新增人" align="center" width="130" prop="createBy" />
      <el-table-column label="修改人" align="center" width="130" prop="updateBy" />
      <el-table-column label="新增时间" align="center" prop="createTime" width="170">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.createTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="修改时间" align="center" prop="updateTime" width="170">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.updateTime) }}</span>
        </template>
      </el-table-column>
    </el-table>

    <pagination v-show="total>0" :total="total" :page.sync="queryParams.pageNum" :limit.sync="queryParams.pageSize"
      @pagination="getList" />

    <!-- 添加或修改预警配置对话框 -->
    <el-dialog :title="title" :visible.sync="open" width="950px" :close-on-click-modal="false">
      <el-form ref="form" :model="form" :rules="rules" label-width="100px">
        <el-form-item label="预警名称" prop="warningName" class="el-col-24">
          <el-input v-model="form.warningName" placeholder="请输入预警名称" :disabled="detailViem" />
        </el-form-item>

        <div>
          <el-form-item label="变量包名称" prop="variablePackageId" class="el-col-24">
            <el-select v-model="form.variablePackageId" :disabled="detailViem" class="el-col-24" placeholder="请选择变量包"
              clearable>
              <el-option v-for="dict in managerOptions" :key="dict.variablePackId" :label="dict.variablePackName"
                :value="dict.variablePackId" />
            </el-select>
          </el-form-item>
        </div>

        <div v-for="(item, index) in form.warningContent" :key="index">
          <el-form-item :label="(index === 0 ? '预警内容' : '')" class="el-col-20" required>
            <el-form-item class="el-col-12" :prop="'warningContent.'+index+'.warningConfigIndicatorsId'" :rules="rules.warningContent.warningConfigIndicatorsId">
              <el-select v-model="item.warningConfigIndicatorsId" class="el-col-24" :disabled="detailViem" @change="warningConfigIndicatorsChange"
                placeholder="请选择预警指标" clearable>
                <el-option v-for="dict in indicatorsOptions" :key="dict.dictValue" :label="dict.dictLabel"
                  :value="dict.dictValue" />
              </el-select>
            </el-form-item>
            <!-- 是否展示运算符和阈值 -->
            <div v-if="(item.warningConfigIndicatorsId !== '4')">
              <el-form-item class="el-col-1">
                <el-col>&nbsp</el-col>
              </el-form-item>
              <el-form-item class="el-col-5" :prop="'warningContent.'+index+'.operatorId'" :rules="rules.warningContent.operatorId">
                <el-select v-model="item.operatorId" class="el-col-24" :disabled="detailViem" placeholder="运算符" @change="warningConfigOperatorChange" clearable>
                  <el-option v-for="dict in operatorOptions" :key="dict.dictValue" :label="dict.dictLabel"
                    :value="dict.dictValue" />
                </el-select>
              </el-form-item>
              <el-form-item class="el-col-1">
                <el-col>&nbsp</el-col>
              </el-form-item>
              <el-form-item class="el-col-5" :prop="'warningContent.'+index+'.value'" :rules="rules.warningContent.value">
                <el-input-number v-model="item.value" :value="item.value" controls-position="right" :min="0" style="width: 100%;" placeholder="阈值" :validate-event="true" :disabled="detailViem"/>
              </el-form-item>
            </div>
          </el-form-item>

          <el-form-item class="el-col-4 elementStyle">
            <span>
              <el-button @click="contentAddItem" :disabled="detailViem">
                <i class="el-icon-plus" />
              </el-button>
              <el-button @click="contentDeleteItem(item, index)" :disabled="detailViem || form.warningContent.length === 1">
                <i class="el-icon-minus" />
              </el-button>
            </span>
          </el-form-item>

        </div>

        <el-form-item label="生效时间" class="el-col-24" required>
          <el-form-item prop="warningEffectTime.minTime" class="el-col-6">
            <el-time-picker :picker-options="{format:'HH:mm',selectableRange: '00:00:00 - '+(form.warningEffectTime.maxTime ? getTimeRange(form.warningEffectTime.maxTime,'min') : '22:59')+':00'}" :default-value="new Date(0, 0, 0, 0, 0)" value-format="HH:mm" v-model="form.warningEffectTime.minTime" :disabled="detailViem" placeholder="选择时间" style="width: 100%;">
            </el-time-picker>
          </el-form-item>
          <el-form-item class="el-col-2">
            <el-col class="line" style="text-align: center;">-</el-col>
          </el-form-item>
          <el-form-item prop="warningEffectTime.maxTime" class="el-col-6">
            <el-time-picker :picker-options="{format:'HH:mm',selectableRange: (form.warningEffectTime.minTime ? getTimeRange(form.warningEffectTime.minTime,'max') : '01:00')+':00 - 23:59:00'}" :default-value="new Date(0, 0, 0, 23, 59)" value-format="HH:mm" v-model="form.warningEffectTime.maxTime" :disabled="detailViem" placeholder="选择时间" style="width: 100%;">
            </el-time-picker>
          </el-form-item>
        </el-form-item>

        <div class="el-col-24">
          <el-form-item label="预警频率" prop="warningFrequency" class="el-col-12">
            <el-select v-model="form.warningFrequency" :disabled="detailViem" class="el-col-24" placeholder="请选择预警频率"
              clearable>
              <el-option v-for="dict in frequencyOptions" :key="dict.dictValue" :label="dict.dictLabel"
                :value="dict.dictValue" />
            </el-select>
          </el-form-item>
        </div>

        <div class="el-col-24">
          <el-form-item label="通知方式" prop="warningNoticeType" class="el-col-12">
            <el-checkbox-group v-model="form.warningNoticeType" :disabled="detailViem">
              <el-checkbox v-for="dict in noticeTypeOptions" :label="dict.dictValue" :key="dict.dictValue">{{dict.dictLabel}}</el-checkbox>
            </el-checkbox-group>
          </el-form-item>
        </div>

        <div class="el-col-24">
          <el-form-item label="联系人组" prop="warningNoticeUser" class="el-col-12">
            <treeselect v-model="form.warningNoticeUser" :options="noticeUserOptions" :multiple="true" id="warningNoticeUser" :disableBranchNodes="true"
              :showCount="true" @select="noticeUserSelect" @deselect="noticeUserDeSelect" :clearable="false" placeholder="请选择联系人组" :disabled="detailViem" />
          </el-form-item>
        </div>

        <div class="el-col-24">
          <el-form-item label="备注" prop="description">
            <el-input v-model="form.description" type="textarea" placeholder="请输入备注内容" style="width: 100%"
              :disabled="detailViem" />
          </el-form-item>
        </div>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button type="primary" @click="submitForm('form')" v-show="showComfirm">确 定</el-button>
        <el-button @click="cancel">取 消</el-button>
      </div>
    </el-dialog>

  </div>
</template>

<script>
  import {
    treeselect
  } from "@/api/system/dept";
  import Treeselect from '@riophae/vue-treeselect';
  import '@riophae/vue-treeselect/dist/vue-treeselect.css';
  import {
    listConfig,
    listManager,
    warningRun,
    warningStop,
    getNoticeUserList,
    getTimeRange,
    addWarningConfig,
    updateWarningConfig,
    delWarningConfig,
    detailWarningConfig
  } from "@/api/warning/config.js";
  import {
    isLegitimateName
  } from "@/utils/validate.js";
  export default {
    name: "WarningConfig",
    components: {
      Treeselect
    },
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
        // 预警配置表格数据
        warningConfigList: [],
        // 弹出层标题
        title: "",
        // 是否显示弹出层
        open: false,
        // 预警指标列表
        indicatorsOptions: [],
        // 预警通知类型
        noticeTypeOptions: [],
        // 预警通知联系人列表
        noticeUserOptions: [],
        // 预警频率列表
        frequencyOptions: [],
        // 预警指标运算符
        operatorOptions: [],
        // 变量包列表
        managerOptions: [],
        // 预警状态
        warningStateOptions: [],
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
          warningState: undefined
        },
        // 表单参数
        form: {
          inputParam: undefined,
          warningEffectTime: {
            minTime: undefined,
            maxTime: undefined
          }
        },
        // 表单校验
        rules: {
          warningName: [{
            required: true,
            message: "预警名称不能为空",
            trigger: "blur"
          }],
          variablePackageId: [{
            required: true,
            message: "请选择变量包",
            trigger: "blur"
          }],
          warningContent: {
            warningConfigIndicatorsId: [{
              required: true,
              message: "请选择预警指标",
              trigger: "blur"
            }],
            operatorId: [{
              required: true,
              message: "请选择指标运算符",
              trigger: "blur"
            }],
            value: [{
              required: true,
              message: "请输入阈值",
              trigger: "blur"
            }],
          },
          warningEffectTime: {
            minTime: [{
              required: true,
              message: "生效时间不能为空",
              trigger: "blur"
            }],
            maxTime: [{
              required: true,
              message: "生效时间不能为空",
              trigger: "blur"
            }]
          },
          warningFrequency: [{
            required: true,
            message: "请选择预警频率",
            trigger: "blur"
          }],
          warningNoticeType: [{
            required: true,
            message: "请选择通知方式",
            trigger: "blur"
          }],
          warningNoticeUser: [{
            required: true,
            message: "请选择联系人组",
            trigger: "blur"
          }]
        }
      };
    },
    created() {
      this.getList();
      this.getManagerList();
      this.getTreeselect();
      this.getDicts("warning_config_indicators").then(response => {
        this.indicatorsOptions = response.data;
      });
      this.getDicts("warning_config_notice_type").then(response => {
        this.noticeTypeOptions = response.data;
      });
      this.getDicts("warning_frequency").then(response => {
        this.frequencyOptions = response.data;
      });
      this.getDicts("warning_config_indicators_operator").then(response => {
        this.operatorOptions = response.data;
      });
      this.getDicts("warning_state").then(response => {
        this.warningStateOptions = response.data;
      });
    },
    methods: {
      /** 查询预警配置列表 */
      getList() {
        this.loading = true;
        listConfig(this.queryParams).then(response => {
          this.warningConfigList = response.rows;
          this.warningConfigList.map((item, key) => {
            item.warningNoticeType = JSON.parse(item.warningNoticeType);
          })
          this.total = response.total;
          this.loading = false;
        });
      },
      /** 查询变量包管理列表 */
      getManagerList() {
        listManager().then(response => {
          this.managerOptions = response.rows;
        });
      },
      /** 查询预警联系人下拉树结构 */
      async getTreeselect() {
        var users = await getNoticeUserList().then(response => {
          return response.rows;
        })
        var deptOptions = await treeselect().then(response => {
          return response.data;
        });
        deptOptions.map((item,index) => {
          this.userToDeptOptions(item, users)
        })
        if(users.length > 0){
          var userArr = [];
          users.map((item,index) => {
            userArr.push({
              "id": item.userId,
              "label": item.userName
            })
          })
          deptOptions.push({
            "id": "otherUsers",
            "label": "其他",
            "children": userArr
          })
        }
        this.noticeUserOptions = deptOptions;
      },
      // 拼接用户到部门
      userToDeptOptions(item, users) {
        if(item.children && item.children.length){
          item.children.map((value, key) => {
            value = this.userToDeptOptions(value, users);
          })
        }
        else{
          item.children = [];
          users.map((value, key) => {
            if(value.deptId && value.deptId === item.id){
              item.children.push({
                "id": value.userId,
                "label": value.userName
              })
              users.splice(key, 1);
            }
          })
        }
        return item.children;
      },
      // 选择预警联系人
      noticeUserSelect(node, instanceId) {
        this.form.warningNoticeUser.push(node.id);
      },
      // 取消选择预警联系人
      noticeUserDeSelect(node, instanceId) {
        if(this.form.warningNoticeUser.indexOf(node.id) != -1){
          this.form.warningNoticeUser.splice(this.form.warningNoticeUser.indexOf(node.id), 1);
        }
      },
      warningConfigIndicatorsChange(value) {
        this.form.warningContent.map((item, key) => {
          this.indicatorsOptions.map((item2, key2) => {
            if(item.warningConfigIndicatorsId === item2.dictValue){
              item.warningConfigIndicatorsName = item2.dictLabel;
            }
          })
        })
      },
      warningConfigOperatorChange(value) {
        this.form.warningContent.map((item, key) => {
          this.operatorOptions.map((item2, key2) => {
            if(item.operatorId === item2.dictValue){
              item.operatorName = item2.dictLabel;
            }
          })
        })
      },
      getTimeRange(time, method) {
        return getTimeRange(time, method);
      },
      // 内容增加行
      contentAddItem() {
        this.form.warningContent.push({
          "warningConfigIndicatorsId": undefined,
          "warningConfigIndicatorsName": undefined,
          "operatorId": undefined,
          "operatorName": undefined,
          "value": undefined
        });
      },
      // 删除行
      contentDeleteItem(item, index) {
        if (this.form.warningContent && this.form.warningContent.length === 1) {
          this.$alert("不能删除所有的字段", {
            type: "error"
          });
          return false;
        }
        this.form.warningContent.splice(index, 1);
      },
      NoticeTypeFormat(row, column) {
        var result = "";
        var data = row.warningNoticeType;
        row.warningNoticeType.map((item, key) => {
          result = result + (result.length > 0 ? "、" : "" ) + this.selectDictLabel(this.noticeTypeOptions, item)
        })
        return result;
      },
      warningStateFormat(row, column) {
        return this.selectDictLabel(this.warningStateOptions, row.warningNoticeType);
      },
      // 预警状态的启动与停用
      handleStatusChange(row) {
        let that = this;
        if (row.warningState === '1') { // 启动
          let loading = that.$loading({
            lock: true,
            text: '正在启用，请耐心等待...',
            spinner: 'el-icon-loading',
            background: 'rgba(0, 0, 0, 0.2)'
          });
          warningRun(row).then(res => {
            loading.close();
            if (res.msg === "success") {
              that.$alert("启用成功！", {
                type: 'warning'
              });
              that.getList();
            } else if (res.msg === "failure") {
              that.$alert("启用失败！", {
                type: 'warning'
              });
              row.warningState = '0'; // 取消按钮不变
            }
          })
        } else if (row.warningState === '0') { // 停用
          this.$confirm('您确定要停用此预警吗, 是否继续?', '提示', {
            confirmButtonText: '确定',
            cancelButtonText: '取消',
            type: 'warning'
          }).then(() => {
            let loading = that.$loading({
              lock: true,
              text: '正在停用，请耐心等待...',
              spinner: 'el-icon-loading',
              background: 'rgba(0, 0, 0, 0.2)'
            });
            warningStop(row).then(res => {
              loading.close();
              if (res.msg === "success") {
                that.$alert("停用成功！", {
                  type: 'warning'
                });
                that.getList();
              } else if (res.msg === "failure") {
                that.$alert("停用失败！", {
                  type: 'warning'
                });
                row.warningState = '1'; // 取消按钮不变
              }
            })
          }).catch(() => {
            row.warningState = '1';
          });
        }
      },
      // 取消按钮
      cancel() {
        this.open = false;
        this.reset();
      },
      // 表单重置
      reset() {
        this.form = {
          "warningName": undefined,
          "variablePackageId": undefined,
          "warningContent": [{
            "warningConfigIndicatorsId": undefined,
            "warningConfigIndicatorsName": undefined,
            "operatorId": undefined,
            "operatorName": undefined,
            "value": undefined
          }],
          "warningEffectTime": {
            "minTime": "",
            "maxTime": ""
          },
          "warningFrequency": undefined,
          "warningNoticeType": [],
          "warningNoticeUser": [],
          "description": undefined
        };
        this.resetForm("form");
        this.showComfirm = true;
        this.detailViem = false;

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
        var ids = [];
        selection.map((item, key) => {
          ids.push(item.warningId);
        })
        this.ids = ids;
        if(this.ids.length > 0){
          this.single = false;
          this.multiple = false;
          if(this.ids.length > 1){
            this.single = true;
          }
        }
        else{
          this.single = true;
          this.multiple = true;
        }
      },
      /** 新增按钮操作 */
      handleAdd() {
        this.reset();
        this.open = true;
        this.title = "添加预警规则";
      },
      /** 修改按钮操作 */
      handleUpdate(row) {
        this.reset();
        const warningConfigId = row.warningId || this.ids[0];
        detailWarningConfig(warningConfigId).then(response => {
          this.form = response.data;
          this.form.warningNoticeType = JSON.parse(this.form.warningNoticeType);
          this.form.warningContent = JSON.parse(this.form.warningContent);
          this.form.warningEffectTime = JSON.parse(this.form.warningEffectTime);
          this.form.warningNoticeUser = JSON.parse(this.form.warningNoticeUser);
          this.open = true;
          this.title = "修改预警规则";
        })
      },
      /** 提交按钮 */
      submitForm: function(form) {
        this.$refs[form].validate(valid => {
          if (valid) {
            if(this.form.warningId !== undefined){
              updateWarningConfig(this.form).then(response => {
                if (response.code === 200) {
                  this.msgSuccess("修改成功");
                  this.open = false;
                  this.getList();
                } else {
                  this.msgError(response.msg);
                }
              });
            }
            else{
              addWarningConfig(this.form).then(response => {
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
        const warningIds = row.warningId || this.ids;
        this.$confirm('是否确认删除预警编号为"' + warningIds + '"的数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function() {
          return delWarningConfig(warningIds);
        }).then(() => {
          this.getList();
          this.msgSuccess("删除成功");
        }).catch(function() {});
      },
      /** 详情按钮操作 */
      handleDetail(row) {
        this.reset();
        const warningConfigId = row.warningId || this.ids[0];
        detailWarningConfig(warningConfigId).then(response => {
          this.form = response.data;
          this.form.warningNoticeType = JSON.parse(this.form.warningNoticeType);
          this.form.warningContent = JSON.parse(this.form.warningContent);
          this.form.warningEffectTime = JSON.parse(this.form.warningEffectTime);
          this.form.warningNoticeUser = JSON.parse(this.form.warningNoticeUser);
          this.open = true;
          this.detailViem = true;
          this.showComfirm = false;
          this.title = "修改预警规则";
        })
      },
    }
  };
</script>

<style scoped>
  .elementStyle>>>.el-form-item__content {
    margin-left: 10px !important;
  }
</style>
