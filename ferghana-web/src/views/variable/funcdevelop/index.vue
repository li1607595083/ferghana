<template>
  <div class="app-container">
    <el-form :model="queryParams" ref="queryForm" :inline="true" label-width="90px">
      <el-form-item label="函数中文名" prop="selfFunctionNameCn">
        <el-input
          v-model="queryParams.selfFunctionNameCn"
          placeholder="请输入函数中文名"
          clearable
          size="small"
          @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item label="函数英文名" prop="functionName">
        <el-input
          v-model="queryParams.functionName"
          placeholder="请输入函数英文名"
          clearable
          size="small"
          @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item label="函数描述" prop="selfFunctionDesc">
        <el-input
          v-model="queryParams.selfFunctionDesc"
          placeholder="请输入函数描述"
          clearable
          size="small"
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
    </el-row>

    <el-table v-loading="loading" :data="functionList" @selection-change="handleSelectionChange">
      <el-table-column type="selection" width="55" align="center"/>
      <el-table-column label="函数中文名" align="center" prop="selfFunctionNameCn"/>
      <el-table-column label="函数英文名" align="center" prop="functionName"/>
      <el-table-column label="函数类型" align="center" prop="functionType" :formatter="functionTypeFormat"/>
      <el-table-column label="函数描述" align="center" prop="selfFunctionDesc"/>
      <el-table-column label="创建时间" align="center" prop="createTime" width="180">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.createTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="修改时间" align="center" prop="updateTime" width="180">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.updateTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="操作" align="center" class-name="small-padding fixed-width">
        <template slot-scope="scope">
          <el-button
            size="mini"
            type="text"
            icon="el-icon-view"
            @click="handleDetail(scope.row)"
          >详情
          </el-button>
          <el-button
            size="mini"
            type="text"
            icon="el-icon-edit"
            @click="handleUpdate(scope.row)"
          >修改
          </el-button>
          <el-button
            size="mini"
            type="text"
            icon="el-icon-delete"
            @click="handleDelete(scope.row)"
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

    <!-- 添加或修改自定义函数对话框 -->
    <el-dialog :title="title" :visible.sync="open" width="950px" :close-on-click-modal="false">
      <el-form ref="form" :model="form" :rules="rules" label-width="100px">
        <el-form-item label="函数中文名" prop="selfFunctionNameCn" class="el-col-12">
          <el-input v-model="form.selfFunctionNameCn" placeholder="请输入函数中文名" :disabled="detailViem"/>
        </el-form-item>
        <el-form-item label="函数名" prop="functionName" class="el-col-12">
          <el-input v-model="form.functionName" placeholder="请输入函数名" :disabled="detailViem"/>
        </el-form-item>
        <el-form-item label="函数路径" prop="functionPackagePath" class="el-col-12">
          <el-input v-model="form.functionPackagePath" placeholder="请输入函数路径" :disabled="detailViem"/>
        </el-form-item>
        <el-form-item label="函数描述" prop="selfFunctionDesc" class="el-col-12">
          <el-input v-model="form.selfFunctionDesc" placeholder="请输入函数路径" :disabled="detailViem"/>
        </el-form-item>
        <el-form-item label="函数类型" prop="functionType" class="el-col-24">
          <el-select v-model="form.functionType" :disabled="detailViem" style="width:355px" placeholder="请输入函数类型">
            <el-option
              v-for="dict in functionTypeOptions"
              :key="dict.dictValue"
              :label="dict.dictLabel"
              :value="dict.dictValue"
            />
          </el-select>
        </el-form-item>
        <!-- 动态增加项目 -->
        <!-- 不止一项，用div包裹起来 -->
        <div>
          <span style="font-size: 15px;font-weight: 700;margin-left: 30px">输入参数：</span>
        </div>
        <div v-for="(item, index) in form.dynamicItem" :key="index" style="padding-top: 10px">
          <el-form-item class="el-col-5 elementStyle" :prop="'dynamicItem.' + index + '.schemaDefine'"
                        >
            <el-input v-model="item.schemaDefine" placeholder="请输入字段" style="width: 160px" :disabled="detailViem"/>
          </el-form-item>

          <el-form-item class="el-col-5 elementStyle" :prop="'dynamicItem.' + index + '.dataBaseType'">
            <el-select v-model="item.dataBaseType" placeholder="请选择数据类型" clearable style="width: 160px"
                       :disabled="detailViem">
              <el-option v-for="dict in sysDataBaseTypes" :key="dict.dictValue" :label="dict.dictLabel"
                         :value="dict.dictLabel"/>
            </el-select>
          </el-form-item>
          <el-form-item class="el-col-5 elementStyle" :prop="'dynamicItem.' + index + '.schemaName'">
            <el-input v-model="item.schemaName" placeholder="请输入中文名" style="width: 160px;" :disabled="detailViem"/>
          </el-form-item>

          <el-form-item class="el-col-9 elementStyle" :prop="'dynamicItem.' + index + '.fieldType'">
            <el-select v-model="item.fieldType" placeholder="请选择字段类型" clearable style="width: 160px"
                       :disabled="detailViem">
              <el-option v-for="dict in sysFieldTypeTypes" :key="dict.dictValue" :label="dict.dictLabel"
                         :value="dict.dictValue"/>
            </el-select>
            <span style="margin-left: 5px">
              <el-button @click="addItem" :disabled="detailViem">
                <i class="el-icon-plus"/>
              </el-button>
              <el-button @click="deleteItem(item, index)" :disabled="detailViem">
                <i class="el-icon-minus"/>
              </el-button>
            </span>
          </el-form-item>
        </div>

        <!-- 动态增加项目 -->
        <!-- 不止一项，用div包裹起来 -->
        <div>
          <span style="font-size: 15px;font-weight: 700;margin-left: 30px">输出参数：</span>
        </div>
        <div v-for="(item, index) in form.dynamicItemOut" :key="index" style="padding-top: 10px">
          <el-form-item class="el-col-5 elementStyle" :prop="'dynamicItemOut.' + index + '.schemaDefine'"
                        :rules="rules.dynamicItemOut.schemaDefine">
            <el-input v-model="item.schemaDefine" placeholder="请输入字段" style="width: 160px" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item class="el-col-5 elementStyle" :prop="'dynamicItemOut.' + index + '.dataBaseType'"
                        :rules="rules.dynamicItemOut.dataBaseType">
            <el-select v-model="item.dataBaseType" placeholder="请选择数据类型" clearable style="width: 160px"
                       :disabled="detailViem">
              <el-option v-for="dict in sysDataBaseTypes" :key="dict.dictValue" :label="dict.dictLabel"
                         :value="dict.dictValue"/>
            </el-select>
          </el-form-item>
          <el-form-item class="el-col-14 elementStyle" :prop="'dynamicItemOut.' + index + '.schemaName'"
                        :rules="rules.dynamicItemOut.schemaName">
            <el-input v-model="item.schemaName" placeholder="请输入中文名" style="width: 160px;" :disabled="detailViem"/>
            <span style="margin-left: 10px">
              <el-button @click="addItemOut" :disabled="detailViem">
                <i class="el-icon-plus"/>
              </el-button>
              <el-button @click="deleteItemOut(item, index)" :disabled="detailViem">
                <i class="el-icon-minus"/>
              </el-button>
            </span>
          </el-form-item>
        </div>
        <!--        <el-form-item label="函数包路径" prop="filePath"  class="el-col-12">-->
        <!--          <el-input v-model="form.filePath" placeholder="请输入函数包路径" />-->
        <!--        </el-form-item>-->
        <div style="width: 300px;margin-left: 30px">
          <el-upload
            :disabled="detailViem"
            class="upload-demo"
            ref="upload"
            :action="uploadAddress()"
            :on-preview="handlePreview"
            :on-remove="handleRemove"
            :file-list="fileList"
            :on-change="handleChange"
            :auto-upload="false">
            <el-button slot="trigger" size="small" type="primary">选取Jar包</el-button>
            <!--          <el-button style="margin-left: 10px;" size="small" type="primary" @click="submitUpload">上传到服务器</el-button>-->
            <div slot="tip" class="el-upload__tip">只能上传jar包,且只能传一个</div>
          </el-upload>
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
    list,
    getFunction,
    delFunction,
    addFunction,
    updateFunction,
    exportFunction,
    uploadJar,
    updateFunctionNoJar,
    getTSelfFunction,
    checkJar
  } from "@/api/taskdevelop/function.js";
  import{isLegitimateName} from "@/utils/validate.js";

  export default {
    name: "Function",
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
        // 自定义函数表格数据
        functionList: [],
        // 弹出层标题
        title: "",
        // 是否显示弹出层
        open: false,
        // 模板类型字典
        moduleTypeOptions: [],
        fileList: [],
        // 数据类型
        sysDataBaseTypes: [],
        // 字段类型
        sysFieldTypeTypes: [],
        // 函数类型options
        functionTypeOptions: [],
        // 更换过jar文件
        confirmJar: false,
        // 详情不展示确定按钮
        showComfirm: true,
        // 详情不可修改
        detailViem: false,
        // 查询参数
        queryParams: {
          pageNum: 1,
          pageSize: 10,
          selfFunctionNameCn: undefined,
          moduleType: undefined,
          functionName: undefined,
          selfFunctionDesc: undefined
        },
        // 表单参数
        form: {inputParam: undefined},
        // 表单校验
        rules: {
          selfFunctionNameCn: [
            {required: true, message: "函数中文名不能为空", trigger: "blur"}
          ],
          selfFunctionDesc: [
            {required: true, message: "函数描述不能为空", trigger: "blur"}
          ],
          functionType: [
            {required: true, message: "函数类型不能为空", trigger: "blur"}
          ],
          functionName: [
            {required: true, message: "函数英文名不能为空", trigger: "blur"},
            {validator: isLegitimateName, message: "格式：只能包含英文字母、数字、下划线且首字母必须是英文字母", trigger: "blur"},
          ],
          functionPackagePath: [
            {required: true, message: "函数路径不能为空", trigger: "blur"}
          ],
          filePath: [
            {required: true, message: "函数包路径不能为空", trigger: "blur"}
          ],
          dynamicItem: {
            schemaDefine: [
              {required: true, message: "字段不能为空", trigger: "blur"}
            ],
            dataBaseType: [
              {required: true, message: "数据类型不能为空", trigger: "blur"}
            ],
            fieldType: [
              {required: true, message: "字段类型不能为空", trigger: "blur"}
            ],
            schemaName: [
              {required: true, message: "中文名不能为空", trigger: "blur"}
            ]
          },
          dynamicItemOut: {
            schemaDefine: [
              {required: true, message: "字段不能为空", trigger: "blur"}
            ],
            dataBaseType: [
              {required: true, message: "数据类型不能为空", trigger: "blur"}
            ],
            schemaName: [
              {required: true, message: "中文名不能为空", trigger: "blur"}
            ]
          }
        }
      };
    },
    created() {
      this.getList();
      this.getDicts("self_func_module_type").then(response => {
        this.moduleTypeOptions = response.data;
      });
      this.getDicts("sys_function_type").then(response => {
        this.functionTypeOptions = response.data;
      });
      this.getDicts("sys_func_param_type").then(response => {
        this.sysDataBaseTypes = response.data;
      });
      this.getDicts("sys_function_field_type").then(response => {
        this.sysFieldTypeTypes = response.data;
      });
    },
    methods: {
      // 增加行
      addItem() {
        this.form.dynamicItem.push({
          schemaDefine: "",
          fieldType: "",
          dataBaseType: "",
          schemaName: ""
        });
      },
      // 删除行
      deleteItem(item, index) {
        if (this.form.dynamicItem && this.form.dynamicItem.length === 1) {
          this.$alert("不能删除所有的字段", {type: "error"});
          return false;
        }
        this.form.dynamicItem.splice(index, 1);
      },
      // 增加行
      addItemOut() {
        this.form.dynamicItemOut.push({
          schemaDefine: "",
          dataBaseType: "",
          schemaName: ""
        });
      },
      // 删除行
      deleteItemOut(item, index) {
        if (this.form.dynamicItemOut && this.form.dynamicItemOut.length === 1) {
          this.$alert("不能删除所有的字段", {type: "error"});
          return false;
        }
        this.form.dynamicItemOut.splice(index, 1);
      },
      // 上传地址
      uploadAddress() {
        return "/variable/funcdevelop/upload";
      },

      submitUpload() {
        console.log(this);
        let file = this.$refs.upload.uploadFiles[0];
        let formData = new FormData();
        formData.append("functionJar", file.raw);
        uploadJar(formData).then((res) => {
          console.log(res);
        });
        // this.$refs.upload.submit();
      },
      handleRemove(file, fileList) {
        // 只要是删除过，就是要修改jar
        this.confirmJar = true;
        console.log(file, fileList);
      },
      handlePreview(file) {
        console.log(file);
      },
      handleChange(file, fileList) {
        if (fileList.length > 1) {
          // 只要是删除过，就是要修改jar
          this.confirmJar = true;
          this.fileList = [fileList[fileList.length - 1]]  // 这一步，是 展示最后一次选择的csv文件
        }
        console.log(this.fileList);
      },

      /** 查询自定义函数列表 */
      getList() {
        this.loading = true;
        list(this.queryParams).then(response => {
          this.functionList = response.rows;
          this.total = response.total;
          this.loading = false;
        });
      },
      // 模板类型字典翻译
      moduleTypeFormat(row, column) {
        return this.selectDictLabel(this.moduleTypeOptions, row.moduleType);
      },
      // 取消按钮
      cancel() {
        this.open = false;
        this.reset();
      },
      // 表单重置
      reset() {
        this.form = {
          selfFunctionId: undefined,
          selfFunctionNameCn: undefined,
          moduleType: undefined,
          functionName: undefined,
          functionPackagePath: undefined,
          filePath: undefined,
          functionParams: undefined,
          selfFunctionDesc: undefined,
          functionType: undefined,
          dynamicItem: [
            {
              schemaDefine: "",
              fieldType: "",
              dataBaseType: "",
              schemaName: ""
            }
          ],
          dynamicItemOut: [
            {
              schemaDefine: "",
              dataBaseType: "",
              schemaName: ""
            }
          ]
        };
        this.resetForm("form");
        this.fileList = [];
        this.confirmJar = false;
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
        this.ids = selection.map(item => item.selfFunctionId);
        this.single = selection.length !== 1;
        this.multiple = !selection.length
      },
      /** 新增按钮操作 */
      handleAdd() {
        this.reset();
        this.open = true;
        this.title = "添加自定义函数";
      },
      /** 修改按钮操作 */
      handleUpdate(row) {
        this.reset();
        const selfFunctionId = row.selfFunctionId || this.ids;
        getFunction(selfFunctionId).then(response => {
          this.form = response.data;
          console.log("---");
          console.log(this.form);
          let filePath = this.form.filePath;
          let number = filePath.lastIndexOf("/");
          this.fileList = [{
            name: filePath.substr(number + 1, filePath.length),
            url: filePath.substr(0, number + 1)
          }];
          this.open = true;
          this.title = "修改自定义函数";
        });
      },
      /** 提交按钮 */
      submitForm: function (form) {
        this.$refs[form].validate(valid => {
          if (valid) {
            if (this.form.selfFunctionId !== undefined) {
              if (this.$refs.upload.uploadFiles.length === 0) {
                this.$alert("请选择需上传的Jar包", {type: "warning"});
                return false;
              }
              let formData = new FormData();
              formData.append("selfFunctionId", this.form.selfFunctionId);
              formData.append("selfFunctionNameCn", this.form.selfFunctionNameCn);
              formData.append("selfFunctionDesc", this.form.selfFunctionDesc);
              formData.append("functionName", this.form.functionName);
              formData.append("functionType", this.form.functionType);
              formData.append("functionPackagePath", this.form.functionPackagePath);
              formData.append("inputParam", JSON.stringify(this.form.dynamicItem));
              formData.append("outputParam", JSON.stringify(this.form.dynamicItemOut));
              if (this.confirmJar) {
                // 如果更换过jar,就需修改jar，否则不用
                let file = this.$refs.upload.uploadFiles[0];
                formData.append("functionJar", file.raw);
                let formData2 = new FormData();
                formData2.append("functionJar", file.raw);
                // 没出结果，一直等待
                let loading = this.$loading({
                  lock: true,
                  text: '正在上传，请耐心等待...',
                  spinner: 'el-icon-loading',
                  background: 'rgba(0, 0, 0, 0.2)'
                });
                // 保存前先校验文件是否已存在
                checkJar(formData2).then((res) => {
                  if (res.msg === "exists") {
                    this.$confirm('该文件已经存在，是否覆盖？', "警告", {
                      confirmButtonText: "确定",
                      cancelButtonText: "取消",
                      type: "warning"
                    }).then(() => {
                      updateFunction(formData).then(response => {
                        loading.close();
                        if (response.code === 200) {
                          this.msgSuccess("修改成功");
                          this.open = false;
                          this.getList();
                        } else {
                          this.msgError(response.msg);
                        }
                      }).catch(()=>{
                        loading.close();
                      });
                    }).catch(()=>{
                      // 取消
                      loading.close();
                    });
                  } else {
                    updateFunction(formData).then(response => {
                      loading.close();
                      if (response.code === 200) {
                        this.msgSuccess("修改成功");
                        this.open = false;
                        this.getList();
                      } else {
                        this.msgError(response.msg);
                      }
                    }).catch(()=>{
                      loading.close();
                    });
                  }

                }).catch(()=>{
                  loading.close();
                });
              } else {
                updateFunctionNoJar(formData).then(response => {
                  if (response.code === 200) {
                    this.msgSuccess("修改成功");
                    this.open = false;
                    this.getList();
                  } else {
                    this.msgError(response.msg);
                  }
                }).catch(()=>{
                });
              }
            } else {
              if (this.$refs.upload.uploadFiles.length === 0) {
                this.$alert("请选择需上传的Jar包", {type: "warning"});
                return false;
              }
              let file = this.$refs.upload.uploadFiles[0];
              let formData = new FormData();
              formData.append("functionJar", file.raw);
              // 没出结果，一直等待
              let loading = this.$loading({
                lock: true,
                text: '正在上传，请耐心等待...',
                spinner: 'el-icon-loading',
                background: 'rgba(0, 0, 0, 0.2)'
              });
              // 保存前先校验文件是否已存在
              checkJar(formData).then((res) => {
                formData.append("selfFunctionNameCn", this.form.selfFunctionNameCn);
                formData.append("selfFunctionDesc", this.form.selfFunctionDesc);
                formData.append("functionName", this.form.functionName);
                formData.append("functionType", this.form.functionType);
                formData.append("functionPackagePath", this.form.functionPackagePath);
                formData.append("inputParam", JSON.stringify(this.form.dynamicItem));
                formData.append("outputParam", JSON.stringify(this.form.dynamicItemOut));
                if (res.msg === "exists") {
                  this.$confirm('该文件已经存在，是否覆盖？', "警告", {
                    confirmButtonText: "确定",
                    cancelButtonText: "取消",
                    type: "warning"
                  }).then(() => {
                    uploadJar(formData).then((response) => {
                      loading.close();
                      if (response.code === 200) {
                        this.msgSuccess("新增成功");
                        this.open = false;
                        this.getList();
                      } else {
                        this.msgError(response.msg);
                      }
                    }).catch(()=>{
                      loading.close();
                    });
                  }).catch(()=>{
                    // 取消
                    loading.close();
                  });
                } else {
                  uploadJar(formData).then((response) => {
                    loading.close();
                    if (response.code === 200) {
                      this.msgSuccess("新增成功");
                      this.open = false;
                      this.getList();
                    } else {
                      this.msgError(response.msg);
                    }
                  }).catch(()=>{
                    loading.close();
                  });
                }
              }).catch(()=>{
                loading.close();
              });
            }
          }
        });
      },
      // 连接器类型字典翻译
      functionTypeFormat(row, column) {
        return this.selectDictLabel(this.functionTypeOptions, row.functionType);
      },
      /** 删除按钮操作 */
      handleDelete(row) {
        const selfFunctionIds = row.selfFunctionId || this.ids;
        this.$confirm('是否确认删除自定义函数编号为"' + selfFunctionIds + '"的数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function () {
          return delFunction(selfFunctionIds);
        }).then(() => {
          this.getList();
          this.msgSuccess("删除成功");
        }).catch(function () {
        });
      },
      /** 详情按钮操作 */
      handleDetail(row) {
        this.reset();
        const selfFunctionId = row.selfFunctionId || this.ids;
        getTSelfFunction(selfFunctionId).then(response => {
          this.showComfirm = false;
          this.form = response.data;
          let filePath = this.form.filePath;
          let number = filePath.lastIndexOf("/");
          this.fileList = [{
            name: filePath.substr(number + 1, filePath.length),
            url: filePath.substr(0, number + 1)
          }];
          this.open = true;
          this.title = "查看自定义函数";
          this.detailViem = true;
        });
      },
      /** 导出按钮操作 */
      handleExport() {
        const queryParams = this.queryParams;
        this.$confirm('是否确认导出所有自定义函数数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function () {
          return exportFunction(queryParams);
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
    margin-left: 45px !important;
  }
</style>

