namespace HP.Celeriq.Configuration
{
    partial class MainForm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.wizard1 = new nHydrate.Wizard.Wizard();
            this.pageConnect = new nHydrate.Wizard.WizardPage();
            this.connectionControl1 = new Celeriq.Configuration.ConnectionControl();
            this.optExistingDatabase = new System.Windows.Forms.RadioButton();
            this.optNewDatabase = new System.Windows.Forms.RadioButton();
            this.pageSummary = new nHydrate.Wizard.WizardPage();
            this.pageFileGroups = new nHydrate.Wizard.WizardPage();
            this.pageSetupNew = new nHydrate.Wizard.WizardPage();
            this.label1 = new System.Windows.Forms.Label();
            this.numericUpDown1 = new System.Windows.Forms.NumericUpDown();
            this.label2 = new System.Windows.Forms.Label();
            this.wizard1.SuspendLayout();
            this.pageConnect.SuspendLayout();
            this.pageSetupNew.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.numericUpDown1)).BeginInit();
            this.SuspendLayout();
            // 
            // wizard1
            // 
            this.wizard1.ButtonFlatStyle = System.Windows.Forms.FlatStyle.Standard;
            this.wizard1.Controls.Add(this.pageSetupNew);
            this.wizard1.Controls.Add(this.pageConnect);
            this.wizard1.Controls.Add(this.pageSummary);
            this.wizard1.Controls.Add(this.pageFileGroups);
            this.wizard1.Location = new System.Drawing.Point(0, 0);
            this.wizard1.Name = "wizard1";
            this.wizard1.Size = new System.Drawing.Size(419, 308);
            this.wizard1.TabIndex = 0;
            this.wizard1.WizardPages.AddRange(new nHydrate.Wizard.WizardPage[] {
            this.pageConnect,
            this.pageSetupNew,
            this.pageFileGroups,
            this.pageSummary});
            // 
            // pageConnect
            // 
            this.pageConnect.Controls.Add(this.connectionControl1);
            this.pageConnect.Controls.Add(this.optExistingDatabase);
            this.pageConnect.Controls.Add(this.optNewDatabase);
            this.pageConnect.Location = new System.Drawing.Point(0, 0);
            this.pageConnect.Name = "pageConnect";
            this.pageConnect.Size = new System.Drawing.Size(419, 260);
            this.pageConnect.TabIndex = 7;
            // 
            // connectionControl1
            // 
            this.connectionControl1.Location = new System.Drawing.Point(12, 109);
            this.connectionControl1.Name = "connectionControl1";
            this.connectionControl1.Size = new System.Drawing.Size(305, 131);
            this.connectionControl1.TabIndex = 2;
            // 
            // optExistingDatabase
            // 
            this.optExistingDatabase.AutoSize = true;
            this.optExistingDatabase.Location = new System.Drawing.Point(152, 75);
            this.optExistingDatabase.Name = "optExistingDatabase";
            this.optExistingDatabase.Size = new System.Drawing.Size(110, 17);
            this.optExistingDatabase.TabIndex = 1;
            this.optExistingDatabase.Text = "Existing Database";
            this.optExistingDatabase.UseVisualStyleBackColor = true;
            // 
            // optNewDatabase
            // 
            this.optNewDatabase.AutoSize = true;
            this.optNewDatabase.Checked = true;
            this.optNewDatabase.Location = new System.Drawing.Point(12, 75);
            this.optNewDatabase.Name = "optNewDatabase";
            this.optNewDatabase.Size = new System.Drawing.Size(96, 17);
            this.optNewDatabase.TabIndex = 0;
            this.optNewDatabase.TabStop = true;
            this.optNewDatabase.Text = "New Database";
            this.optNewDatabase.UseVisualStyleBackColor = true;
            // 
            // pageSummary
            // 
            this.pageSummary.Location = new System.Drawing.Point(0, 0);
            this.pageSummary.Name = "pageSummary";
            this.pageSummary.Size = new System.Drawing.Size(566, 333);
            this.pageSummary.TabIndex = 9;
            // 
            // pageFileGroups
            // 
            this.pageFileGroups.Location = new System.Drawing.Point(0, 0);
            this.pageFileGroups.Name = "pageFileGroups";
            this.pageFileGroups.Size = new System.Drawing.Size(566, 333);
            this.pageFileGroups.TabIndex = 10;
            // 
            // pageSetupNew
            // 
            this.pageSetupNew.Controls.Add(this.label2);
            this.pageSetupNew.Controls.Add(this.numericUpDown1);
            this.pageSetupNew.Controls.Add(this.label1);
            this.pageSetupNew.Location = new System.Drawing.Point(0, 0);
            this.pageSetupNew.Name = "pageSetupNew";
            this.pageSetupNew.Size = new System.Drawing.Size(419, 260);
            this.pageSetupNew.TabIndex = 8;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(13, 75);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(91, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Growth Increment";
            // 
            // numericUpDown1
            // 
            this.numericUpDown1.Increment = new decimal(new int[] {
            10,
            0,
            0,
            0});
            this.numericUpDown1.Location = new System.Drawing.Point(136, 75);
            this.numericUpDown1.Maximum = new decimal(new int[] {
            500,
            0,
            0,
            0});
            this.numericUpDown1.Minimum = new decimal(new int[] {
            10,
            0,
            0,
            0});
            this.numericUpDown1.Name = "numericUpDown1";
            this.numericUpDown1.Size = new System.Drawing.Size(103, 20);
            this.numericUpDown1.TabIndex = 1;
            this.numericUpDown1.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
            this.numericUpDown1.Value = new decimal(new int[] {
            10,
            0,
            0,
            0});
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(246, 79);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(23, 13);
            this.label2.TabIndex = 2;
            this.label2.Text = "MB";
            // 
            // MainForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(419, 308);
            this.Controls.Add(this.wizard1);
            this.Name = "MainForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Celeriq Configuration";
            this.wizard1.ResumeLayout(false);
            this.wizard1.PerformLayout();
            this.pageConnect.ResumeLayout(false);
            this.pageConnect.PerformLayout();
            this.pageSetupNew.ResumeLayout(false);
            this.pageSetupNew.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.numericUpDown1)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private nHydrate.Wizard.Wizard wizard1;
        private nHydrate.Wizard.WizardPage pageConnect;
        private System.Windows.Forms.RadioButton optExistingDatabase;
        private System.Windows.Forms.RadioButton optNewDatabase;
        private nHydrate.Wizard.WizardPage pageSummary;
        private nHydrate.Wizard.WizardPage pageFileGroups;
        private nHydrate.Wizard.WizardPage pageSetupNew;
        private ConnectionControl connectionControl1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.NumericUpDown numericUpDown1;
        private System.Windows.Forms.Label label1;
    }
}

