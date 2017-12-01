namespace Gravitybox.Datastore.Install
{
	partial class InstallSettingsUI
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
			System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(InstallSettingsUI));
			this.chkIgnoreWarnings = new System.Windows.Forms.CheckBox();
			this.chkUseTransaction = new System.Windows.Forms.CheckBox();
			this.chkSkipNormalize = new System.Windows.Forms.CheckBox();
			this.cmdHelp = new System.Windows.Forms.Button();
			this.chkUseHash = new System.Windows.Forms.CheckBox();
			this.SuspendLayout();
			// 
			// chkIgnoreWarnings
			// 
			this.chkIgnoreWarnings.AutoSize = true;
			this.chkIgnoreWarnings.Location = new System.Drawing.Point(3, 3);
			this.chkIgnoreWarnings.Name = "chkIgnoreWarnings";
			this.chkIgnoreWarnings.Size = new System.Drawing.Size(170, 17);
			this.chkIgnoreWarnings.TabIndex = 20;
			this.chkIgnoreWarnings.Text = "Suppress Versioning Warnings";
			this.chkIgnoreWarnings.UseVisualStyleBackColor = true;
			// 
			// chkUseTransaction
			// 
			this.chkUseTransaction.AutoSize = true;
			this.chkUseTransaction.Location = new System.Drawing.Point(3, 26);
			this.chkUseTransaction.Name = "chkUseTransaction";
			this.chkUseTransaction.Size = new System.Drawing.Size(104, 17);
			this.chkUseTransaction.TabIndex = 21;
			this.chkUseTransaction.Text = "Use Transaction";
			this.chkUseTransaction.UseVisualStyleBackColor = true;
			// 
			// chkSkipNormalize
			// 
			this.chkSkipNormalize.AutoSize = true;
			this.chkSkipNormalize.Location = new System.Drawing.Point(3, 49);
			this.chkSkipNormalize.Name = "chkSkipNormalize";
			this.chkSkipNormalize.Size = new System.Drawing.Size(113, 17);
			this.chkSkipNormalize.TabIndex = 22;
			this.chkSkipNormalize.Text = "Skip Normalization";
			this.chkSkipNormalize.UseVisualStyleBackColor = true;
			// 
			// cmdHelp
			// 
			this.cmdHelp.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
			this.cmdHelp.Image = ((System.Drawing.Image)(resources.GetObject("cmdHelp.Image")));
			this.cmdHelp.Location = new System.Drawing.Point(346, 105);
			this.cmdHelp.Name = "cmdHelp";
			this.cmdHelp.Size = new System.Drawing.Size(24, 24);
			this.cmdHelp.TabIndex = 24;
			this.cmdHelp.UseVisualStyleBackColor = true;
			this.cmdHelp.Click += new System.EventHandler(this.cmdHelp_Click);
			// 
			// chkUseHash
			// 
			this.chkUseHash.AutoSize = true;
			this.chkUseHash.Location = new System.Drawing.Point(3, 72);
			this.chkUseHash.Name = "chkUseHash";
			this.chkUseHash.Size = new System.Drawing.Size(84, 17);
			this.chkUseHash.TabIndex = 23;
			this.chkUseHash.Text = "Use Hashes";
			this.chkUseHash.UseVisualStyleBackColor = true;
			// 
			// InstallSettingsUI
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.Controls.Add(this.cmdHelp);
			this.Controls.Add(this.chkUseHash);
			this.Controls.Add(this.chkSkipNormalize);
			this.Controls.Add(this.chkUseTransaction);
			this.Controls.Add(this.chkIgnoreWarnings);
			this.Name = "InstallSettingsUI";
			this.Size = new System.Drawing.Size(373, 132);
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.CheckBox chkIgnoreWarnings;
		private System.Windows.Forms.CheckBox chkUseTransaction;
		private System.Windows.Forms.CheckBox chkSkipNormalize;
		private System.Windows.Forms.Button cmdHelp;
		private System.Windows.Forms.CheckBox chkUseHash;
	}
}
