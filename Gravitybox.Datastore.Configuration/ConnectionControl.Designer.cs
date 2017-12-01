namespace Gravitybox.Datastore.Configuration
{
    partial class ConnectionControl
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

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.txtPassword = new System.Windows.Forms.TextBox();
            this.txtUserName = new System.Windows.Forms.TextBox();
            this.optLoginSecurity = new System.Windows.Forms.RadioButton();
            this.optIntegratedSecurity = new System.Windows.Forms.RadioButton();
            this.labelCreationPassword = new System.Windows.Forms.Label();
            this.labelCreationUserName = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.txtServer = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // txtPassword
            // 
            this.txtPassword.Enabled = false;
            this.txtPassword.Location = new System.Drawing.Point(91, 104);
            this.txtPassword.Name = "txtPassword";
            this.txtPassword.PasswordChar = '*';
            this.txtPassword.Size = new System.Drawing.Size(193, 20);
            this.txtPassword.TabIndex = 4;
            // 
            // txtUserName
            // 
            this.txtUserName.Enabled = false;
            this.txtUserName.Location = new System.Drawing.Point(91, 80);
            this.txtUserName.Name = "txtUserName";
            this.txtUserName.Size = new System.Drawing.Size(193, 20);
            this.txtUserName.TabIndex = 3;
            // 
            // optLoginSecurity
            // 
            this.optLoginSecurity.Location = new System.Drawing.Point(3, 56);
            this.optLoginSecurity.Name = "optLoginSecurity";
            this.optLoginSecurity.Size = new System.Drawing.Size(280, 20);
            this.optLoginSecurity.TabIndex = 2;
            this.optLoginSecurity.Text = "Use a specific user name and password";
            // 
            // optIntegratedSecurity
            // 
            this.optIntegratedSecurity.Checked = true;
            this.optIntegratedSecurity.Location = new System.Drawing.Point(3, 32);
            this.optIntegratedSecurity.Name = "optIntegratedSecurity";
            this.optIntegratedSecurity.Size = new System.Drawing.Size(280, 20);
            this.optIntegratedSecurity.TabIndex = 1;
            this.optIntegratedSecurity.TabStop = true;
            this.optIntegratedSecurity.Text = "Use Windows NT Integrated security";
            // 
            // labelCreationPassword
            // 
            this.labelCreationPassword.Location = new System.Drawing.Point(19, 104);
            this.labelCreationPassword.Name = "labelCreationPassword";
            this.labelCreationPassword.Size = new System.Drawing.Size(64, 16);
            this.labelCreationPassword.TabIndex = 30;
            this.labelCreationPassword.Text = "Password:";
            // 
            // labelCreationUserName
            // 
            this.labelCreationUserName.Location = new System.Drawing.Point(19, 80);
            this.labelCreationUserName.Name = "labelCreationUserName";
            this.labelCreationUserName.Size = new System.Drawing.Size(64, 16);
            this.labelCreationUserName.TabIndex = 29;
            this.labelCreationUserName.Text = "User Name: ";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(4, 4);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(41, 13);
            this.label1.TabIndex = 31;
            this.label1.Text = "Server:";
            // 
            // txtServer
            // 
            this.txtServer.Location = new System.Drawing.Point(91, 4);
            this.txtServer.Name = "txtServer";
            this.txtServer.Size = new System.Drawing.Size(192, 20);
            this.txtServer.TabIndex = 0;
            // 
            // ConnectionControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.txtServer);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.txtPassword);
            this.Controls.Add(this.txtUserName);
            this.Controls.Add(this.optLoginSecurity);
            this.Controls.Add(this.optIntegratedSecurity);
            this.Controls.Add(this.labelCreationPassword);
            this.Controls.Add(this.labelCreationUserName);
            this.Name = "ConnectionControl";
            this.Size = new System.Drawing.Size(305, 137);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox txtPassword;
        private System.Windows.Forms.TextBox txtUserName;
        private System.Windows.Forms.RadioButton optLoginSecurity;
        private System.Windows.Forms.RadioButton optIntegratedSecurity;
        private System.Windows.Forms.Label labelCreationPassword;
        private System.Windows.Forms.Label labelCreationUserName;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox txtServer;
    }
}
