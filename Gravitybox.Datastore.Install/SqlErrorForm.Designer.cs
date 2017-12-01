namespace Gravitybox.Datastore.Install
{
	partial class SqlErrorForm
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
			this.panel1 = new System.Windows.Forms.Panel();
			this.txtError = new System.Windows.Forms.TextBox();
			this.label1 = new System.Windows.Forms.Label();
			this.panel2 = new System.Windows.Forms.Panel();
			this.cmdClose = new System.Windows.Forms.Button();
			this.splitter1 = new System.Windows.Forms.Splitter();
			this.panel3 = new System.Windows.Forms.Panel();
			this.txtSql = new System.Windows.Forms.TextBox();
			this.label2 = new System.Windows.Forms.Label();
			this.cmdSkip = new System.Windows.Forms.Button();
			this.panel1.SuspendLayout();
			this.panel2.SuspendLayout();
			this.panel3.SuspendLayout();
			this.SuspendLayout();
			// 
			// panel1
			// 
			this.panel1.Controls.Add(this.txtError);
			this.panel1.Controls.Add(this.label1);
			this.panel1.Dock = System.Windows.Forms.DockStyle.Top;
			this.panel1.Location = new System.Drawing.Point(0, 6);
			this.panel1.Name = "panel1";
			this.panel1.Padding = new System.Windows.Forms.Padding(10, 3, 10, 10);
			this.panel1.Size = new System.Drawing.Size(450, 168);
			this.panel1.TabIndex = 0;
			// 
			// txtError
			// 
			this.txtError.Dock = System.Windows.Forms.DockStyle.Fill;
			this.txtError.Location = new System.Drawing.Point(10, 26);
			this.txtError.Multiline = true;
			this.txtError.Name = "txtError";
			this.txtError.ReadOnly = true;
			this.txtError.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
			this.txtError.Size = new System.Drawing.Size(430, 132);
			this.txtError.TabIndex = 1;
			// 
			// label1
			// 
			this.label1.Dock = System.Windows.Forms.DockStyle.Top;
			this.label1.Location = new System.Drawing.Point(10, 3);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(430, 23);
			this.label1.TabIndex = 0;
			this.label1.Text = "Error:";
			this.label1.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
			// 
			// panel2
			// 
			this.panel2.Controls.Add(this.cmdSkip);
			this.panel2.Controls.Add(this.cmdClose);
			this.panel2.Dock = System.Windows.Forms.DockStyle.Bottom;
			this.panel2.Location = new System.Drawing.Point(0, 355);
			this.panel2.Name = "panel2";
			this.panel2.Size = new System.Drawing.Size(450, 47);
			this.panel2.TabIndex = 1;
			// 
			// cmdClose
			// 
			this.cmdClose.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
			this.cmdClose.DialogResult = System.Windows.Forms.DialogResult.Cancel;
			this.cmdClose.Location = new System.Drawing.Point(363, 12);
			this.cmdClose.Name = "cmdClose";
			this.cmdClose.Size = new System.Drawing.Size(75, 23);
			this.cmdClose.TabIndex = 0;
			this.cmdClose.Text = "Close";
			this.cmdClose.UseVisualStyleBackColor = true;
			// 
			// splitter1
			// 
			this.splitter1.Dock = System.Windows.Forms.DockStyle.Top;
			this.splitter1.Location = new System.Drawing.Point(0, 0);
			this.splitter1.MaximumSize = new System.Drawing.Size(0, 200);
			this.splitter1.Name = "splitter1";
			this.splitter1.Size = new System.Drawing.Size(450, 6);
			this.splitter1.TabIndex = 2;
			this.splitter1.TabStop = false;
			// 
			// panel3
			// 
			this.panel3.Controls.Add(this.txtSql);
			this.panel3.Controls.Add(this.label2);
			this.panel3.Dock = System.Windows.Forms.DockStyle.Fill;
			this.panel3.Location = new System.Drawing.Point(0, 174);
			this.panel3.Name = "panel3";
			this.panel3.Padding = new System.Windows.Forms.Padding(10, 3, 10, 10);
			this.panel3.Size = new System.Drawing.Size(450, 181);
			this.panel3.TabIndex = 3;
			// 
			// txtSql
			// 
			this.txtSql.Dock = System.Windows.Forms.DockStyle.Fill;
			this.txtSql.Location = new System.Drawing.Point(10, 26);
			this.txtSql.Margin = new System.Windows.Forms.Padding(0);
			this.txtSql.Multiline = true;
			this.txtSql.Name = "txtSql";
			this.txtSql.ReadOnly = true;
			this.txtSql.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
			this.txtSql.Size = new System.Drawing.Size(430, 145);
			this.txtSql.TabIndex = 2;
			// 
			// label2
			// 
			this.label2.Dock = System.Windows.Forms.DockStyle.Top;
			this.label2.Location = new System.Drawing.Point(10, 3);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(430, 23);
			this.label2.TabIndex = 1;
			this.label2.Text = "SQL:";
			this.label2.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
			// 
			// cmdSkip
			// 
			this.cmdSkip.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
			this.cmdSkip.DialogResult = System.Windows.Forms.DialogResult.OK;
			this.cmdSkip.Location = new System.Drawing.Point(282, 12);
			this.cmdSkip.Name = "cmdSkip";
			this.cmdSkip.Size = new System.Drawing.Size(75, 23);
			this.cmdSkip.TabIndex = 3;
			this.cmdSkip.Text = "Skip";
			this.cmdSkip.UseVisualStyleBackColor = true;
			// 
			// SqlErrorForm
			// 
			this.AcceptButton = this.cmdClose;
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.BackColor = System.Drawing.SystemColors.Control;
			this.CancelButton = this.cmdClose;
			this.ClientSize = new System.Drawing.Size(450, 402);
			this.Controls.Add(this.panel3);
			this.Controls.Add(this.panel1);
			this.Controls.Add(this.splitter1);
			this.Controls.Add(this.panel2);
			this.MaximizeBox = false;
			this.MinimizeBox = false;
			this.MinimumSize = new System.Drawing.Size(460, 440);
			this.Name = "SqlErrorForm";
			this.ShowIcon = false;
			this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
			this.Text = "Installation Error";
			this.panel1.ResumeLayout(false);
			this.panel1.PerformLayout();
			this.panel2.ResumeLayout(false);
			this.panel3.ResumeLayout(false);
			this.panel3.PerformLayout();
			this.ResumeLayout(false);

		}

		#endregion

		private System.Windows.Forms.Panel panel1;
		private System.Windows.Forms.Panel panel2;
		private System.Windows.Forms.Button cmdClose;
		private System.Windows.Forms.Splitter splitter1;
		private System.Windows.Forms.Panel panel3;
		private System.Windows.Forms.TextBox txtError;
		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.TextBox txtSql;
		private System.Windows.Forms.Label label2;
		private System.Windows.Forms.Button cmdSkip;
	}
}